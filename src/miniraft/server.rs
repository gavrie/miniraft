use async_std::prelude::*;
use async_std::sync::{self, Receiver, Sender};
use async_std::task;
use async_std::task::JoinHandle;
use log::{debug, error, info};
use rand::prelude::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::rpc::Message;
use super::state::*;
use crate::miniraft::client::ClientRequest;
use crate::miniraft::rpc::{
    AppendEntriesArguments, AppendEntriesResults, FramedMessage, RequestVoteArguments,
    RequestVoteResults, Target,
};

//////////////////////////////////

struct Follower {
    election_timeout: ElectionTimeout,
}

struct Candidate {
    election_timeout: ElectionTimeout,
}

struct Leader {
    heartbeat_interval: Receiver<()>,
    client_requests: Receiver<ClientRequest>,
    volatile: VolatileDataForLeader,
}

pub trait IsServerState {}
impl IsServerState for Follower {}
impl IsServerState for Candidate {}
impl IsServerState for Leader {}

//////////////////////////////////

// Persistent State on all servers.
// (Updated on stable storage before responding to RPCs)
// TODO: Actually make the data persistent, by disallowing direct mutation and instead
// using mutators that sync to storage.
#[derive(Debug)]
struct PersistentData {
    // Latest Term server has seen (initialized to 0 on first boot, increases monotonically)
    current_term: Term,

    // CandidateId that received vote in current term (or None)
    voted_for: Option<ServerId>,

    // Log entries
    log: Log,
}

// Volatile State on all servers.
#[derive(Debug)]
struct VolatileData {
    // Index of highest log entry known to be committed (initialized to 0, increases monotonically)
    commit_index: LogIndex,

    // Index of highest log entry applied to state machine (initialized to 0, increases monotonically)
    last_applied: LogIndex,
}

// Volatile State on leaders (Reinitialized after election).
#[derive(Debug)]
struct VolatileDataForLeader {
    // For each server, index of the next log entry to send to that server
    // (initialized to leader last log index + 1)
    next_indexes: Vec<LogIndex>,
    // For each server, index of highest log entry known to be replicated on server
    // (initialized to 0, increases monotonically)
    match_indexes: Vec<LogIndex>,
}

#[derive(Debug)]
struct Log(Vec<LogEntry>);

/// The actual log. Indexes are 1-based.
impl Log {
    fn new() -> Self {
        Self(Vec::new())
    }

    fn append(&mut self, mut entries: Vec<LogEntry>) {
        self.0.append(entries.as_mut())
    }

    fn last_index(&self) -> LogIndex {
        LogIndex::new(self.0.len())
    }

    fn last_term(&self) -> Term {
        self.0.last().map(|l| l.term).unwrap_or(Term(0))
    }

    fn get(&self, index: LogIndex) -> Option<&LogEntry> {
        self.0.get(index.zero_based())
    }

    fn truncate(&mut self, index: LogIndex) {
        self.0.truncate(index.zero_based())
    }
}

//////////////////////////////////

pub struct Server {
    pub server_tx: Sender<FramedMessage>,
    pub handle: JoinHandle<()>,
}

impl Server {
    pub fn new(id: ServerId, cluster_tx: Sender<FramedMessage>) -> Self {
        let (server_tx, server_rx) = sync::channel(1);

        info!("{:?}: Started server", id);

        let handle = task::spawn(async move {
            let mut state = ServerState::Follower(State::new(id, cluster_tx, server_rx));
            loop {
                state = state.next().await;
            }
        });

        Self { server_tx, handle }
    }
}

enum ServerState {
    Follower(State<Follower>),
    Candidate(State<Candidate>),
    Leader(State<Leader>),
}

enum FollowerEvent {
    ElectionTimeout(Duration),
    FramedMessage(FramedMessage),
}

enum LeaderEvent {
    Heartbeat(()),
    FramedMessage(FramedMessage),
    ClientRequest(ClientRequest),
}

impl ServerState {
    async fn next(self) -> Self {
        match self {
            ServerState::Follower(state) => Self::handle_follower(state).await,
            ServerState::Candidate(state) => Self::handle_candidate(state).await,
            ServerState::Leader(state) => Self::handle_leader(state).await,
        }
    }

    async fn handle_follower(mut state: State<Follower>) -> Self {
        let mut received_heartbeat = false;

        let mut events = state.events();

        while let Some(event) = events.next().await {
            match event {
                FollowerEvent::FramedMessage(FramedMessage {
                    source,
                    target: _,
                    message,
                }) => {
                    debug!("{:?}: <<< {:?}", state.data.id, message);

                    if state.update_term_if_outdated(message.clone()).await {
                        return ServerState::Follower(state.become_follower());
                    }

                    match message.as_ref() {
                        Message::RequestVoteRequest(args) => {
                            state.vote(args).await;
                        }

                        Message::AppendEntriesRequest(args) => {
                            // Reset election timeout
                            received_heartbeat = true;
                            state.state.election_timeout =
                                state.state.election_timeout.reset_election_timeout().await;

                            // Process entries
                            let success = state.append_entries(&args);

                            // Respond to leader
                            let response = Message::AppendEntriesResponse(AppendEntriesResults {
                                term: state.data.persistent.current_term,
                                success,
                            });

                            state
                                .send_message(Arc::new(response), Target::Server(source))
                                .await;
                        }

                        Message::RequestVoteResponse(_results) => {
                            error!("Follower received RequestVote response")
                        }

                        Message::AppendEntriesResponse(_results) => {
                            error!("Follower received AppendEntries response")
                        }
                    }
                }
                FollowerEvent::ElectionTimeout(duration) => {
                    if state.data.persistent.voted_for.is_some() {
                        info!(
                            "{:?}: Election timed out after {:?}, but voted for other candidate",
                            state.data.id, duration,
                        );
                        continue;
                    }

                    if received_heartbeat {
                        info!(
                            "{:?}: Election timed out after {:?}, but received heartbeat",
                            state.data.id, duration,
                        );
                        continue;
                    }

                    info!(
                        "{:?}: Election timed out after {:?}, converting to candidate",
                        state.data.id, duration,
                    );
                    return ServerState::Candidate(state.become_candidate().await);
                }
            }
        }
        panic!("should not get here");
    }

    async fn handle_candidate(mut state: State<Candidate>) -> Self {
        let num_servers = 3; // FIXME: Don't hardcode but get from Cluster
        let mut num_votes = 1; // Vote for self

        info!(
            "{:?}: Voted for self ({}/{})",
            state.data.id, num_votes, num_servers
        );

        let has_quorum = |num_votes| num_votes * 2 > num_servers;

        let mut events = state.events();

        while let Some(event) = events.next().await {
            match event {
                FollowerEvent::FramedMessage(FramedMessage {
                    source,
                    target: _,
                    message,
                }) => {
                    debug!("{:?}: <<< {:?}", state.data.id, message);

                    if state.update_term_if_outdated(message.clone()).await {
                        return ServerState::Follower(state.become_follower());
                    }

                    match message.as_ref() {
                        Message::RequestVoteRequest(_args) => {}
                        Message::RequestVoteResponse(results) => {
                            if results.vote_granted {
                                num_votes += 1;

                                info!(
                                    "{:?}: Received vote from {:?} ({}/{})",
                                    state.data.id, source, num_votes, num_servers
                                );

                                if has_quorum(num_votes) {
                                    info!(
                                        "{:?}: Got majority of {}/{} votes",
                                        state.data.id, num_votes, num_servers
                                    );
                                    return ServerState::Leader(state.become_leader());
                                }
                            }
                        }
                        Message::AppendEntriesRequest(args) => {
                            // If another legitimate leader appeared, we recognize it
                            if args.term >= state.data.persistent.current_term {
                                return ServerState::Follower(state.become_follower());
                            }
                        }
                        Message::AppendEntriesResponse(_results) => {
                            error!("Candidate received AppendEntries response");
                        }
                    }
                }
                FollowerEvent::ElectionTimeout(duration) => {
                    debug!(
                        "{:?}: Election timed out after {:?}, starting new election",
                        state.data.id, duration,
                    );
                    return ServerState::Candidate(state.start_election().await);
                }
            }
        }

        panic!("should not get here");
    }

    async fn handle_leader(mut state: State<Leader>) -> Self {
        let mut events = state.events();

        while let Some(event) = events.next().await {
            match event {
                LeaderEvent::FramedMessage(FramedMessage {
                    source: _,
                    target: _,
                    message,
                }) => {
                    debug!("{:?}: <<< {:?}", state.data.id, message);

                    if state.update_term_if_outdated(message.clone()).await {
                        return ServerState::Follower(state.become_follower());
                    }

                    match message.as_ref() {
                        Message::RequestVoteRequest(_args) => {
                            error!("Leader received RequestVote request");
                        }
                        Message::RequestVoteResponse(_results) => {
                            debug!("Leader received RequestVote response, ignoring");
                        }
                        Message::AppendEntriesRequest(_args) => {
                            error!("Leader received AppendEntries request");
                        }
                        Message::AppendEntriesResponse(results) => {
                            info!(
                                "Leader received AppendEntries response: {}",
                                results.success
                            );

                            // TODO: Add identifier to tie response to request
                            // (keep in mind that the response may be to a heartbeat)

                            // TODO: Handle response (5.3):
                            // - If successful, update next_index and match_index for follower
                            // - If failed, decrement next_index and retry
                        }
                    }
                }

                LeaderEvent::Heartbeat(_) => {
                    state.send_heartbeat().await;
                }

                LeaderEvent::ClientRequest(request) => {
                    info!("{:?}: <<< {:?}", state.data.id, request);

                    let log = &mut state.data.persistent.log;

                    // Append new entry
                    let entries = vec![LogEntry {
                        command: request.command,
                        term: state.data.persistent.current_term,
                    }];

                    log.append(entries.clone());

                    // Send to other servers
                    let message = Message::AppendEntriesRequest(AppendEntriesArguments {
                        term: state.data.persistent.current_term,
                        leader_id: state.data.id,
                        entries,
                        prev_log_index: log.last_index(),
                        prev_log_term: log.last_term(),
                        leader_commit: state.data.volatile.commit_index,
                    });

                    state.send_message(Arc::new(message), Target::All).await;
                }
            }

            // TODO: Replicate any pending log entries to clients

            // TODO: Respond to pending clients after entries are committed and applied locally
        }

        panic!("should not get here");
    }
}

#[derive(Debug)]
pub struct State<S: IsServerState> {
    state: S,
    data: ServerData,
}

#[derive(Debug)]
struct ServerData {
    id: ServerId,
    persistent: PersistentData,
    volatile: VolatileData,
    sender: Sender<FramedMessage>,
    receiver: Receiver<FramedMessage>,
}

impl<S: IsServerState> State<S> {
    async fn send_message(&self, message: Arc<Message>, target: Target) {
        debug!("{:?}: >>> [{:?}] {:?}", self.data.id, target, message);

        self.data
            .sender
            .send(FramedMessage {
                source: self.data.id,
                target,
                message,
            })
            .await;
    }

    async fn update_term_if_outdated(&mut self, message: Arc<Message>) -> bool {
        let term = match message.as_ref() {
            Message::RequestVoteRequest(args) => args.term,
            Message::RequestVoteResponse(results) => results.term,
            Message::AppendEntriesRequest(args) => args.term,
            Message::AppendEntriesResponse(results) => results.term,
        };

        let current_term = self.data.persistent.current_term;

        if term > current_term {
            info!(
                "{:?}: Found newer term ({}) than our current one ({})",
                self.data.id, term.0, current_term.0
            );

            self.data.persistent.current_term = term;
            self.data.persistent.voted_for = None;

            // Resend the original message so that it's processed after the state transition
            self.send_message(message, Target::Server(self.data.id))
                .await;

            true
        } else {
            false
        }
    }

    fn become_follower2(self) -> State<Follower> {
        State {
            state: Follower {
                election_timeout: ElectionTimeout::new(self.data.id),
            },
            data: self.data,
        }
    }
}

impl State<Follower> {
    pub fn new(
        id: ServerId,
        sender: Sender<FramedMessage>,
        receiver: Receiver<FramedMessage>,
    ) -> Self {
        Self {
            state: Follower {
                election_timeout: ElectionTimeout::new(id),
            },
            data: ServerData {
                id,
                persistent: PersistentData::new(),
                volatile: VolatileData::new(),
                sender,
                receiver,
            },
        }
    }

    // TODO: Use a generic impl common to both Follower and Candidate
    fn events(&self) -> impl Stream<Item = FollowerEvent> {
        // Wait for RPC request or response, or election timeout
        let messages = self.data.receiver.clone().map(FollowerEvent::FramedMessage);

        let election_timeout = self
            .state
            .election_timeout
            .receiver
            .clone()
            .map(FollowerEvent::ElectionTimeout);

        let events = election_timeout.merge(messages);
        events
    }

    fn become_follower(self) -> State<Follower> {
        info!("{:?}: Staying follower", self.data.id);
        self.become_follower2()
    }

    async fn become_candidate(self) -> State<Candidate> {
        info!("{:?}: Becoming candidate", self.data.id);

        let state = State {
            state: Candidate {
                election_timeout: ElectionTimeout::new(self.data.id),
            },
            data: self.data,
        };

        state.start_election().await
    }

    async fn vote(&mut self, args: &RequestVoteArguments) {
        let vote_granted = {
            if args.term < self.data.persistent.current_term {
                info!(
                    "{:?}: Requested vote with stale term {:?}, refusing",
                    self.data.id, args.term
                );
                false
            } else {
                let voted_for = self.data.persistent.voted_for;

                if voted_for == None || voted_for == Some(args.candidate_id) {
                    // TODO: Check if candidate's log is at least as up-to-date as receiver's log
                    info!(
                        "{:?}: Granting vote to {:?}",
                        self.data.id, args.candidate_id
                    );
                    self.data.persistent.voted_for = Some(args.candidate_id);
                    true
                } else {
                    info!(
                        "{:?}: Already voted for {:?}, not granting",
                        self.data.id, args.candidate_id
                    );
                    false
                }
            }
        };

        let server_id = args.candidate_id;
        info!(
            "{:?}: Voting for {:?}: granted={}",
            self.data.id, server_id, vote_granted
        );

        let message = Message::RequestVoteResponse(RequestVoteResults {
            term: self.data.persistent.current_term,
            vote_granted,
        });

        self.send_message(Arc::new(message), Target::Server(server_id))
            .await;
    }

    fn append_entries(&mut self, args: &AppendEntriesArguments) -> bool {
        let log = &mut self.data.persistent.log;

        // Figure 2: Receiver implementation

        // 1. Require at least current term
        if args.term < self.data.persistent.current_term {
            return false;
        }

        // 2. Check safety property
        if args.prev_log_index != LogIndex::ZERO {
            if let Some(entry) = log.get(args.prev_log_index) {
                // Ensure previous entry has right term
                if entry.term != args.prev_log_term {
                    return false;
                }
            }
        }

        // 3. Discard any conflicting local entries
        let mut index = args.prev_log_index + 1;
        for entry in args.entries.iter() {
            if let Some(existing_entry) = log.get(index) {
                if existing_entry.term != entry.term {
                    // Remove conflicting entries
                    log.truncate(args.prev_log_index);
                    return false;
                }
            }
            index += 1;
        }

        // 4. Append any missing new entries
        log.append(args.entries.clone());

        // 5. Handle commits
        if args.leader_commit > self.data.volatile.commit_index {
            self.data.volatile.commit_index = args.leader_commit.min(log.last_index());
        }

        if !args.entries.is_empty() {
            info!(
                "{:?}: Current log [committed: {:?}: {:?}",
                self.data.id, self.data.volatile.commit_index, log
            );
        }

        // We made it here successfully
        true
    }
}

impl State<Candidate> {
    async fn start_election(mut self) -> Self {
        info!("{:?}: Starting election", self.data.id);

        let persistent = &mut self.data.persistent;

        persistent.current_term += 1;
        persistent.voted_for = None;

        let message = Message::RequestVoteRequest(RequestVoteArguments {
            term: persistent.current_term,
            candidate_id: self.data.id,
            last_log_index: persistent.log.last_index(),
            last_log_term: persistent.log.last_term(),
        });

        self.send_message(Arc::new(message), Target::All).await;

        self
    }

    fn events(&self) -> impl Stream<Item = FollowerEvent> {
        // Wait for RPC request or response, or election timeout
        let messages = self.data.receiver.clone().map(FollowerEvent::FramedMessage);

        let election_timeout = self
            .state
            .election_timeout
            .receiver
            .clone()
            .map(FollowerEvent::ElectionTimeout);

        let events = election_timeout.merge(messages);
        events
    }

    fn become_follower(self) -> State<Follower> {
        info!("{:?}: Becoming follower", self.data.id);
        self.become_follower2()
    }

    fn become_leader(self) -> State<Leader> {
        info!("{:?}: Becoming leader", self.data.id);

        let state = State {
            state: Leader {
                volatile: VolatileDataForLeader {
                    next_indexes: vec![], // TODO: Initialize for all servers
                    match_indexes: vec![],
                },
                heartbeat_interval: State::start_heartbeat(self.data.id),
                client_requests: State::start_simulated_client_requests(self.data.id),
            },
            data: ServerData {
                id: self.data.id,
                persistent: self.data.persistent,
                volatile: self.data.volatile,
                sender: self.data.sender,
                receiver: self.data.receiver,
            },
        };

        state
    }
}

impl State<Leader> {
    fn start_heartbeat(server_id: ServerId) -> Receiver<()> {
        let interval = Duration::from_millis(50);

        let (tx, rx) = sync::channel(1);

        debug!("{:?}: Set heartbeat interval to {:?}", server_id, interval);

        task::spawn(async move {
            loop {
                tx.send(()).await;
                task::sleep(interval).await;
            }
        });

        rx
    }

    fn start_simulated_client_requests(server_id: ServerId) -> Receiver<ClientRequest> {
        let interval = Duration::from_secs(3600);
        //let interval = Duration::from_millis(1000);

        let (tx, rx) = sync::channel(1);

        let mut i = 1;

        let _handle = task::spawn(async move {
            loop {
                let request = ClientRequest {
                    command: Command(format!("set x={}", i)),
                };

                info!(
                    "{:?}: Sending simulated client request: {:?}",
                    server_id, request
                );
                tx.send(request).await;
                i += 1;

                task::sleep(interval).await;
            }
        });

        rx
    }

    fn events(&self) -> impl Stream<Item = LeaderEvent> {
        // Wait for RPC request or response, or election timeout
        let messages = self.data.receiver.clone().map(LeaderEvent::FramedMessage);

        let timeouts = self
            .state
            .heartbeat_interval
            .clone()
            .map(LeaderEvent::Heartbeat);

        let client_requests = self
            .state
            .client_requests
            .clone()
            .map(LeaderEvent::ClientRequest);

        let events = messages.merge(timeouts).merge(client_requests);
        events
    }

    async fn send_heartbeat(&self) {
        debug!("{:?}: Sending heartbeat", self.data.id);

        let persistent = &self.data.persistent;

        let message = Message::AppendEntriesRequest(AppendEntriesArguments {
            term: persistent.current_term,
            leader_id: self.data.id,
            entries: vec![],
            prev_log_index: persistent.log.last_index(),
            prev_log_term: persistent.log.last_term(),
            leader_commit: self.data.volatile.commit_index,
        });

        self.send_message(Arc::new(message), Target::All).await;
    }

    fn become_follower(self) -> State<Follower> {
        info!("{:?}: Becoming follower", self.data.id);
        self.become_follower2()
    }
}

struct ElectionTimeout {
    server_id: ServerId,
    receiver: Receiver<Duration>,
    handle: JoinHandle<()>,
}

impl ElectionTimeout {
    fn new(server_id: ServerId) -> Self {
        let (receiver, handle) = Self::set_election_timeout(server_id);

        Self {
            server_id,
            receiver,
            handle,
        }
    }

    async fn reset_election_timeout(mut self) -> Self {
        // Cancel previous timeout first
        self.handle.cancel().await;
        let server_id = self.server_id;

        let (rx, handle) = Self::set_election_timeout(server_id);

        self.receiver = rx;
        self.handle = handle;

        self
    }

    fn set_election_timeout(server_id: ServerId) -> (Receiver<Duration>, JoinHandle<()>) {
        let timeout = Duration::from_millis(thread_rng().gen_range(150, 300));

        let (tx, rx) = sync::channel(1);
        let start = Instant::now();

        debug!("{:?}: Set election timeout to {:?}", server_id, timeout);

        let handle = task::spawn(async move {
            task::sleep(timeout).await;
            tx.send(start.elapsed()).await;
        });

        (rx, handle)
    }
}

impl PersistentData {
    fn new() -> Self {
        Self {
            current_term: Term(0),
            voted_for: None,
            log: Log::new(),
        }
    }
}

impl VolatileData {
    fn new() -> Self {
        Self {
            commit_index: LogIndex::ZERO,
            last_applied: LogIndex::ZERO,
        }
    }
}
