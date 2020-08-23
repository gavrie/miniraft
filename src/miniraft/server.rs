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
use crate::miniraft::rpc::{FramedMessage, RequestVoteArguments, RequestVoteResults, Target};

//////////////////////////////////

struct Follower;
struct Candidate;
struct Leader(VolatileDataForLeader);

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

enum Event {
    ElectionTimeout(Duration),
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
        use ServerState::*;

        let mut events = state.events();
        while let Some(event) = events.next().await {
            match event {
                Event::FramedMessage(frame) => {
                    debug!("{:?}: <<< {:?}", state.data.id, frame.message);
                    use Message::*;

                    match frame.message.as_ref() {
                        RequestVoteRequest(args) => {
                            state.vote(args).await;
                            state.reset_election_timeout();
                        }
                        RequestVoteResponse(_results) => {}
                        AppendEntriesRequest(_args) => {}
                        AppendEntriesResponse(_results) => {}
                    }
                }
                Event::ElectionTimeout(duration) => {
                    info!(
                        "{:?}: Election timed out after {:?}, converting to candidate",
                        state.data.id, duration,
                    );
                    return Candidate(state.become_candidate().await);
                }
            }
        }
        panic!("should not get here");
    }

    async fn handle_candidate(state: State<Candidate>) -> Self {
        use ServerState::*;

        let num_servers = 3; // FIXME: Don't hardcode but get from Cluster
        let mut num_votes = 1; // Vote for self
        let mut events = state.events();

        while let Some(event) = events.next().await {
            match event {
                Event::FramedMessage(frame) => {
                    debug!("{:?}: <<< {:?}", state.data.id, frame.message);
                    use Message::*;

                    match frame.message.as_ref() {
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
                Event::ElectionTimeout(duration) => {
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

    async fn handle_leader(state: State<Leader>) -> Self {
        use ServerState::*;

        let mut events = state.events();
        while let Some(event) = events.next().await {
            match event {
                Event::FramedMessage(frame) => {
                    debug!("{:?}: <<< {:?}", state.data.id, frame.message);
                    use Message::*;

                    match frame.message.as_ref() {
                        RequestVoteRequest(args) => {
                            state.vote(args).await;
                        }
                        RequestVoteResponse(_results) => {}
                        AppendEntriesRequest(_args) => {}
                        AppendEntriesResponse(_results) => {}
                    }
                }
                Event::ElectionTimeout(_) => {
                    panic!("Election timeout in leader -- should not happen!")
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
    election_timeout: Receiver<Duration>,
    sender: Sender<FramedMessage>,
    receiver: Receiver<FramedMessage>,
}

impl<S: IsServerState> State<S> {
    fn reset_election_timeout(&mut self) {
        let timeout = Duration::from_millis(thread_rng().gen_range(150, 300));

        let (tx, rx) = sync::channel(1);
        self.data.election_timeout = rx;
        let start = Instant::now();

        debug!("{:?}: Set election timeout to {:?}", self.data.id, timeout);

        task::spawn(async move {
            task::sleep(timeout).await;
            tx.send(start.elapsed()).await;
        });
    }

    fn events(&self) -> impl Stream<Item = Event> {
        // Wait for RPC request or response, or election timeout
        let messages = self.data.receiver.clone().map(Event::FramedMessage);

        let timeouts = self
            .data
            .election_timeout
            .clone()
            .map(Event::ElectionTimeout);

        let events = timeouts.merge(messages);
        events
    }

    async fn send_message(&self, message: Message, target: Target) {
        info!("{:?}: >>> [{:?}] {:?}", self.data.id, target, message);

        self.data
            .sender
            .send(FramedMessage {
                source: self.data.id,
                target,
                message: Arc::new(message),
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

        self.send_message(message, Target::Server(server_id)).await;
    }
}

impl State<Follower> {
    pub fn new(
        id: ServerId,
        sender: Sender<FramedMessage>,
        receiver: Receiver<FramedMessage>,
    ) -> Self {
        let mut state = Self {
            state: Follower,
            data: ServerData {
                id,
                persistent: PersistentData::new(),
                volatile: VolatileData::new(),
                election_timeout: sync::channel(1).1,
                sender,
                receiver,
            },
        };

        state.reset_election_timeout();

        state
    }

    async fn become_candidate(self) -> State<Candidate> {
        info!("{:?}: Becoming candidate", self.data.id);

        let state = State {
            state: Candidate,
            data: self.data,
        };

        state.start_election().await
    }
}

impl State<Candidate> {
    async fn start_election(mut self) -> Self {
        info!("{:?}: Starting election", self.data.id);

        self.data.persistent.current_term += 1;

        // TODO: Vote for self

        self.reset_election_timeout();

        // TODO: Send RequestVote RPCs to all other servers

        let message = Message::RequestVoteRequest(RequestVoteArguments::new(
            self.data.persistent.current_term,
            self.data.id,
        ));

        self.send_message(message, Target::All).await;

        self
    }

    fn become_leader(self) -> State<Leader> {
        info!("{:?}: Becoming leader", self.data.id);

        let state = State {
            state: Leader(VolatileDataForLeader::new()),
            data: ServerData {
                id: self.data.id,
                persistent: self.data.persistent,
                volatile: self.data.volatile,
                election_timeout: sync::channel(1).1, // Inert channel
                sender: self.data.sender,
                receiver: self.data.receiver,
            },
        };

        state
    }
}

impl State<Leader> {}

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
