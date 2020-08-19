use async_std::future;
use async_std::sync::{Receiver, Sender};
use async_std::task;
use async_std::task::JoinHandle;
use log::{info, trace};
use rand::prelude::*;
use std::sync::Arc;
use std::time::Duration;

use super::rpc::Message;
use super::state::*;
use crate::miniraft::rpc::RequestVoteArguments;

//////////////////////////////////

trait ServerState {}

struct Follower;
struct Candidate;
struct Leader(VolatileDataForLeader);

impl ServerState for Follower {}
impl ServerState for Candidate {}
impl ServerState for Leader {}

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

#[derive(Debug)]
pub struct Server<S: ServerState> {
    state: S,
    data: ServerData,
}

struct ServerData {
    id: ServerId,
    persistent: PersistentData,
    volatile: VolatileData,
    rng: ThreadRng,
    election_timeout: Duration,
    sender: Sender<(ServerId, Arc<Message>)>,
    receiver: Receiver<Arc<Message>>,
}

impl Server<Follower> {
    pub fn spawn(
        id: ServerId,
        sender: Sender<(ServerId, Arc<Message>)>,
        receiver: Receiver<Arc<Message>>,
    ) -> JoinHandle<()> {
        let mut server = Self {
            state: Follower,
            data: ServerData {
                id,
                persistent: PersistentData::new(),
                volatile: VolatileData::new(),
                rng: thread_rng(),
                election_timeout: Default::default(),
                sender,
                receiver,
            },
        };

        server.reset_election_timeout();

        task::spawn(async move {
            server.run().unwrap();
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("{:?}: Started server", self.id);

        loop {
            // FIXME: Instead of a fixed timeout, use an 'after' channel that will
            // time out only if the election timed out?
            match future::timeout(Duration::from_millis(100), self.receiver.recv()).await {
                Ok(Ok(message)) => {
                    info!("{:?}: <<< {:?}", self.id, message);
                }
                Ok(Err(e)) => return Err(e)?,
                Err(_) => {
                    trace!("{:?}: Timed out", self.id);
                }
            }

            self.run2(); // TODO: Check Result
        }
    }

    async fn run2(&mut self) -> Result<Server<Candidate>> {
        // TODO: Wait for RPC request or response, or election timeout

        task::sleep(self.election_timeout).await;
        info!(
            "{:?}: Election timed out after {:?}",
            self.id, self.election_timeout
        );

        Ok(self.become_candidate())
    }

    fn become_candidate(self) -> Server<Candidate> {
        info!("{:?}: Becoming candidate", self.id);

        // TODO: Transition Server<Follower> to ServerState<Candidate>.
        // Transition by calling a method instead of by setting state explicitly.
        // This will allow us to ensure the transition is a valid one.
        Server {
            state: Candidate,
            data: self.data,
        }
    }

    fn reset_election_timeout(&mut self) {
        let election_timeout = self.rng.gen_range(150, 300);
        self.election_timeout = Duration::from_millis(election_timeout);
    }
}

impl Server<Candidate> {
    fn run(&mut self) {}

    fn start_election(&mut self) -> Result<()> {
        // Starting election:
        self.persistent.current_term += 1;

        // TODO: Vote for self

        self.reset_election_timeout();

        // TODO: Send RequestVote RPCs to all other servers

        let message = Message::RequestVoteRequest(RequestVoteArguments::new(
            self.persistent.current_term,
            self.id,
        ));

        let message = Arc::new(message);
        info!("{:?}: >>> {:?}", self.id, message);
        self.sender.send(message)?;

        Ok(())
    }
}

impl Server<Leader> {
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
