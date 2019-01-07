use std::thread;
use std::time::Duration;
use std::sync::mpsc;

use log::info;
use rand::prelude::*;

use super::state::*;
use super::rpc::Message;

#[derive(Debug)]
pub struct Server {
    id: ServerId,
    state: ServerState,
    persistent: PersistentData,
    volatile: VolatileData,
    rng: ThreadRng,
    election_timeout: Duration,
    sender: mpsc::Sender<Message>,
    receiver: mpsc::Receiver<Message>,
}

#[derive(Debug)]
enum ServerState {
    Follower,
    Candidate,
    Leader(VolatileDataForLeader),
}

// Persistent State on all servers.
// (Updated on stable storage before responding to RPCs)
#[derive(Debug)]
struct PersistentData {
    // Latest Term server has seen (initialized to 0 on first boot, increases monotonically)
    current_term: Term,

    // CandidateId that received vote in current term (or None)
    voted_for: Option<CandidateId>,

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

pub struct ServerChannels {
    sender: mpsc::Sender<Message>,
    receiver: mpsc::Receiver<Message>,
}

impl Server {
    pub fn new(id: ServerId, channels_tx: mpsc::Sender<ServerChannels>) -> Self {
        // Channel on which we send our messages
        let (sender_tx, sender_rx) = mpsc::channel::<Message>();

        // Channel on which we receive our messages
        let (receiver_tx, receiver_rx) = mpsc::channel::<Message>();

        let channels = ServerChannels {
            sender: receiver_tx,
            receiver: sender_rx,
        };

        channels_tx.send(channels).unwrap();

        let mut server = Self {
            id,
            state: ServerState::Follower,
            persistent: PersistentData::new(),
            volatile: VolatileData::new(),
            rng: thread_rng(),
            election_timeout: Default::default(),
            sender: sender_tx,
            receiver: receiver_rx,
        };

        server.reset_election_timeout();
        server
    }

    pub fn start(&mut self) {
        info!("{:?}: Started server", self.id);

        loop {
            thread::sleep(Duration::from_millis(100));

            match self.state {
                ServerState::Follower => {
                    // TODO: Wait for RPC request or response, or election timeout
                    thread::sleep(self.election_timeout);
                    info!("{:?}: Election timed out after {:?}", self.id, self.election_timeout);
                    self.start_election();
                }
                ServerState::Candidate => (),
                ServerState::Leader(_) => (),
            }
        }
    }

    fn start_election(&mut self) {
        info!("{:?}: Becoming candidate", self.id);
        self.state = ServerState::Candidate;

        // Starting election:
        self.persistent.current_term += 1;

        // TODO: Vote for self

        self.reset_election_timeout();

        // TODO: Send RequestVote RPCs to all other servers
    }

    fn reset_election_timeout(&mut self) {
        let election_timeout = self.rng.gen_range(150, 300);
        self.election_timeout = Duration::from_millis(election_timeout);
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
