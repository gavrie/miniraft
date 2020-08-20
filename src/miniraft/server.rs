use async_std::prelude::*;
use async_std::sync::{self, Receiver, Sender};
use async_std::task;
use async_std::task::JoinHandle;
use log::info;
use rand::prelude::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::rpc::Message;
use super::state::*;
use crate::miniraft::rpc::{FramedMessage, RequestVoteArguments, Target};

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
    handle: JoinHandle<()>,
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
    Timeout(Duration),
    FramedMessage(FramedMessage),
}

impl ServerState {
    async fn next(self) -> Self {
        use ServerState::*;

        match self {
            Follower(state) => {
                // Wait for RPC request or response, or election timeout
                let messages = state.data.receiver.clone().map(Event::FramedMessage);
                let timeouts = state.data.election_timeout.clone().map(Event::Timeout);
                let mut events = timeouts.merge(messages);

                while let Some(event) = events.next().await {
                    match event {
                        Event::FramedMessage(frame) => {
                            info!("{:?}: <<< {:?}", state.data.id, frame.message);
                        }
                        Event::Timeout(duration) => {
                            info!(
                                "{:?}: Election timed out after {:?}",
                                state.data.id, duration,
                            );
                            return Candidate(state.become_candidate().await);
                        }
                    }
                }

                panic!("should not get here");
            }

            Candidate(state) => {
                task::sleep(Duration::from_secs(3600)).await;
                Candidate(state)
            }

            Leader(state) => {
                task::sleep(Duration::from_secs(3600)).await;
                Leader(state)
            }
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
    election_timeout: Receiver<Duration>,
    sender: Sender<FramedMessage>,
    receiver: Receiver<FramedMessage>,
}

impl<S: IsServerState> State<S> {
    fn set_election_timeout(&mut self) {
        let timeout = Duration::from_millis(thread_rng().gen_range(150, 300));

        let (tx, rx) = sync::channel(1);
        self.data.election_timeout = rx;
        let start = Instant::now();

        task::spawn(async move {
            task::sleep(timeout).await;
            tx.send(start.elapsed()).await;
        });
    }
}

impl State<Follower> {
    pub fn new(
        id: ServerId,
        cluster_tx: Sender<FramedMessage>,
        receiver: Receiver<FramedMessage>,
    ) -> Self {
        let mut state = Self {
            state: Follower,
            data: ServerData {
                id,
                persistent: PersistentData::new(),
                volatile: VolatileData::new(),
                election_timeout: sync::channel(1).1,
                sender: cluster_tx,
                receiver,
            },
        };

        state.set_election_timeout();

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

        self.set_election_timeout();

        // TODO: Send RequestVote RPCs to all other servers

        let message = Message::RequestVoteRequest(RequestVoteArguments::new(
            self.data.persistent.current_term,
            self.data.id,
        ));

        info!("{:?}: >>> [{:?}] {:?}", self.data.id, Target::All, message);

        self.data
            .sender
            .send(FramedMessage {
                source: self.data.id,
                target: Target::All,
                message: Arc::new(message),
            })
            .await;

        self
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
