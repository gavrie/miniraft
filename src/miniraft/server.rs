use async_std::future;
use async_std::prelude::*;
use async_std::sync::{self, Receiver, Sender};
use async_std::task;
use async_std::task::JoinHandle;
use log::debug;
use log::info;
use rand::prelude::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::rpc::Message;
use super::state::*;
use crate::miniraft::rpc::{
    AppendEntriesArguments, FramedMessage, RequestVoteArguments, RequestVoteResults, Target,
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
    volatile: VolatileDataForLeader,
}

pub trait IsServerState {}
impl IsServerState for Follower {}
impl IsServerState for Candidate {}
impl IsServerState for Leader {}

//////////////////////////////////

// Persistent State on all servers.
// (Updated on stable storage before responding to RPCs)
#[derive(Debug)]
struct PersistentData {
    // Latest Term server has seen (initialized to 0 on first boot, increases monotonically)
    current_term: Term,

    // CandidateId that received vote in current term (or None)
    voted_for: Option<ServerId>,

    // Log entries
    log: Vec<LogEntry>,
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
}

impl ServerState {
    async fn next(self) -> Self {
        use ServerState::*;

        match self {
            Follower(state) => Self::handle_follower(state).await,
            Candidate(state) => Self::handle_candidate(state).await,
            Leader(state) => Self::handle_leader(state).await,
        }
    }

    async fn handle_follower(mut state: State<Follower>) -> Self {
        let mut has_voted = false;
        let mut received_heartbeat = false;

        let mut events = state.events();

        while let Some(event) = events.next().await {
            match event {
                FollowerEvent::FramedMessage(frame) => {
                    debug!("{:?}: <<< {:?}", state.data.id, frame.message);
                    use Message::*;

                    let message = frame.message.as_ref();
                    state.update_term_if_outdated(message);

                    match message {
                        RequestVoteRequest(args) => {
                            has_voted = true;
                            state.vote(args).await;
                        }
                        RequestVoteResponse(_results) => {}
                        AppendEntriesRequest(_args) => {
                            received_heartbeat = true;
                            state.state.election_timeout =
                                state.state.election_timeout.reset_election_timeout().await;
                        }
                        AppendEntriesResponse(_results) => {}
                    }
                }
                FollowerEvent::ElectionTimeout(duration) => {
                    if has_voted {
                        info!(
                            "{:?}: Election timed out after {:?}, but voted for other candidate",
                            state.data.id, duration,
                        );
                    } else if received_heartbeat {
                        info!(
                            "{:?}: Election timed out after {:?}, but received heartbeat",
                            state.data.id, duration,
                        );
                        continue;
                    } else {
                        info!(
                            "{:?}: Election timed out after {:?}, converting to candidate",
                            state.data.id, duration,
                        );
                        return ServerState::Candidate(state.become_candidate().await);
                    }
                }
            }
        }
        panic!("should not get here");
    }

    async fn handle_candidate(mut state: State<Candidate>) -> Self {
        use ServerState::*;

        let num_servers = 3; // FIXME: Don't hardcode but get from Cluster
        let mut num_votes = 1; // Vote for self
        let mut events = state.events();

        while let Some(event) = events.next().await {
            match event {
                FollowerEvent::FramedMessage(frame) => {
                    debug!("{:?}: <<< {:?}", state.data.id, frame.message);
                    use Message::*;

                    let message = frame.message.as_ref();
                    if state.update_term_if_outdated(message) {
                        state
                            .send_message(frame.message, Target::Server(state.data.id))
                            .await;
                        return Follower(state.become_follower());
                    }

                    match message {
                        RequestVoteRequest(args) => {
                            state.vote(args).await;
                        }
                        RequestVoteResponse(results) => {
                            if results.vote_granted {
                                info!("{:?}: Received vote from {:?}", state.data.id, frame.source);
                                num_votes += 1;

                                // Do we have a majority vote?
                                if num_votes >= num_servers / 2 + 1 {
                                    info!(
                                        "{:?}: Got {}/{} votes",
                                        state.data.id, num_votes, num_servers
                                    );
                                    return Leader(state.become_leader());
                                }
                            }
                        }
                        AppendEntriesRequest(_args) => {}
                        AppendEntriesResponse(_results) => {}
                    }
                }
                FollowerEvent::ElectionTimeout(duration) => {
                    debug!(
                        "{:?}: Election timed out after {:?}, starting new election",
                        state.data.id, duration,
                    );
                    return Candidate(state.start_election().await);
                }
            }
        }

        panic!("should not get here");
    }

    async fn handle_leader(mut state: State<Leader>) -> Self {
        use ServerState::*;

        let mut events = state.events();
        while let Some(event) = events.next().await {
            match event {
                LeaderEvent::FramedMessage(frame) => {
                    debug!("{:?}: <<< {:?}", state.data.id, frame.message);
                    use Message::*;

                    let message = frame.message.as_ref();
                    if state.update_term_if_outdated(message) {
                        state
                            .send_message(frame.message, Target::Server(state.data.id))
                            .await;
                        return Follower(state.become_follower());
                    }

                    match message {
                        RequestVoteRequest(args) => {
                            state.vote(args).await;
                        }
                        RequestVoteResponse(_results) => {}
                        AppendEntriesRequest(_args) => {}
                        AppendEntriesResponse(_results) => {}
                    }
                }
                LeaderEvent::Heartbeat(_) => {
                    state.send_heartbeat().await;
                }
            }
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
        info!("{:?}: >>> [{:?}] {:?}", self.data.id, target, message);

        self.data
            .sender
            .send(FramedMessage {
                source: self.data.id,
                target,
                message,
            })
            .await;
    }

    async fn vote(&self, args: &RequestVoteArguments) {
        let voted_for = self.data.persistent.voted_for;
        let vote_granted = {
            if args.term < self.data.persistent.current_term {
                false
            } else {
                (voted_for == None || voted_for == Some(args.candidate_id)) && {
                    // TODO: Check if candidate's log is at least as up-to-date as receiver's log
                    true
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

    fn update_term_if_outdated(&mut self, message: &Message) -> bool {
        let term = match message {
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
            true
        } else {
            false
        }
    }

    fn become_follower(self) -> State<Follower> {
        info!("{:?}: Becoming follower", self.data.id);

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

    async fn become_candidate(self) -> State<Candidate> {
        info!("{:?}: Becoming candidate", self.data.id);

        let mut state = State {
            state: Candidate {
                election_timeout: ElectionTimeout::new(self.data.id),
            },
            data: self.data,
        };

        state.start_election().await
    }
}

impl State<Candidate> {
    async fn start_election(mut self) -> Self {
        info!("{:?}: Starting election", self.data.id);

        self.data.persistent.current_term += 1;

        let message = Message::RequestVoteRequest(RequestVoteArguments::new(
            self.data.persistent.current_term,
            self.data.id,
        ));

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

    fn become_leader(self) -> State<Leader> {
        info!("{:?}: Becoming leader", self.data.id);

        let state = State {
            state: Leader {
                volatile: VolatileDataForLeader::new(),
                heartbeat_interval: State::start_heartbeat(self.data.id),
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

    fn events(&self) -> impl Stream<Item = LeaderEvent> {
        // Wait for RPC request or response, or election timeout
        let messages = self.data.receiver.clone().map(LeaderEvent::FramedMessage);

        let timeouts = self
            .state
            .heartbeat_interval
            .clone()
            .map(LeaderEvent::Heartbeat);

        let events = timeouts.merge(messages);
        events
    }

    async fn send_heartbeat(&self) {
        info!("{:?}: Sending heartbeat", self.data.id);

        let message = Message::AppendEntriesRequest(AppendEntriesArguments {
            term: self.data.persistent.current_term,
            leader_id: self.data.id,
            prev_log_index: LogIndex(0), // TODO
            prev_log_term: Term(0),      // TODO
            entries: vec![],
            leader_commit: LogIndex(0), // TODO
        });

        self.send_message(Arc::new(message), Target::All).await;
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
            log: Vec::new(),
        }
    }
}

impl VolatileData {
    fn new() -> Self {
        Self {
            commit_index: LogIndex(0),
            last_applied: LogIndex(0),
        }
    }
}

impl VolatileDataForLeader {
    fn new() -> Self {
        Self {
            next_indexes: vec![],
            match_indexes: vec![],
        }
    }
}
