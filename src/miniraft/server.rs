use async_std::future;
use async_std::sync::{self, Receiver, Sender};
use async_std::task;
use async_std::task::JoinHandle;
use log::info;
use rand::prelude::*;
use std::sync::Arc;
use std::time::Duration;

use super::rpc::Message;
use super::state::*;
use crate::miniraft::rpc::RequestVoteArguments;

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
    pub server_tx: Sender<Arc<Message>>,
    handle: JoinHandle<()>,
}

impl Server {
    pub fn new(id: ServerId, cluster_tx: Sender<(ServerId, Arc<Message>)>) -> Self {
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

impl ServerState {
    async fn next(self) -> Self {
        use ServerState::*;
        match self {
            Follower(state) => Candidate(state.run().await),
            server_state => server_state,
        }
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
    election_timeout: Duration,
    sender: Sender<(ServerId, Arc<Message>)>,
    receiver: Receiver<Arc<Message>>,
}

impl<S: IsServerState> State<S> {
    fn reset_election_timeout(&mut self) {
        let election_timeout = thread_rng().gen_range(150, 300);
        self.data.election_timeout = Duration::from_millis(election_timeout);
    }
}

impl State<Follower> {
    pub fn new(
        id: ServerId,
        cluster_tx: Sender<(ServerId, Arc<Message>)>,
        receiver: Receiver<Arc<Message>>,
    ) -> Self {
        let mut state = Self {
            state: Follower,
            data: ServerData {
                id,
                persistent: PersistentData::new(),
                volatile: VolatileData::new(),
                election_timeout: Default::default(),
                sender: cluster_tx,
                receiver,
            },
        };

        state.reset_election_timeout();

        state
    }

    async fn run(self) -> State<Candidate> {
        // Wait for RPC request or response, or election timeout

        let message = future::timeout(self.data.election_timeout, self.data.receiver.recv()).await;

        match message {
            Ok(Ok(message)) => {
                info!("{:?}: <<< {:?}", self.data.id, message);
            }
            Ok(Err(e)) => panic!("{}", e),
            Err(_) => {
                info!(
                    "{:?}: Election timed out after {:?}",
                    self.data.id, self.data.election_timeout
                );
            }
        }

        self.become_candidate()
    }

    fn become_candidate(self) -> State<Candidate> {
        info!("{:?}: Becoming candidate", self.data.id);

        // TODO: Transition Server<Follower> to Server<Candidate>.
        // Transition by calling a method instead of by setting state explicitly.
        // This will allow us to ensure the transition is a valid one.
        State {
            state: Candidate,
            data: self.data,
        }
    }
}

impl State<Candidate> {
    fn run(&mut self) {}

    async fn start_election(&mut self) -> Result<()> {
        // Starting election:
        self.data.persistent.current_term += 1;

        // TODO: Vote for self

        self.reset_election_timeout();

        // TODO: Send RequestVote RPCs to all other servers

        let message = Message::RequestVoteRequest(RequestVoteArguments::new(
            self.data.persistent.current_term,
            self.data.id,
        ));

        let message = Arc::new(message);
        info!("{:?}: >>> {:?}", self.data.id, message);
        self.data.sender.send((self.data.id, message)).await;

        Ok(())
    }
}

impl State<Leader> {
    fn run(&mut self) {}
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
