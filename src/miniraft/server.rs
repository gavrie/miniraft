use std::thread;
use std::time::Duration;

use crossbeam_channel::{Sender, Receiver, unbounded};
use log::{trace, info};
use rand::prelude::*;

use super::state::*;
use super::rpc::Message;
use crate::miniraft::rpc::RequestVoteArguments;
use std::error::Error;

#[derive(Debug)]
pub struct Server {
    id: ServerId,
    state: ServerState,
    persistent: PersistentData,
    volatile: VolatileData,
    rng: ThreadRng,
    election_timeout: Duration,
    sender: Sender<Message>,
    receiver: Receiver<Message>,
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

pub struct ServerChannels {
    pub sender: Sender<Message>,
    pub receiver: Receiver<Message>,
}

impl Server {
    pub fn new(id: ServerId, channels_tx: Sender<ServerChannels>)
               -> Result<Self, Box<dyn Error>> {
        // Channel on which we send our messages
        let (sender_tx, sender_rx) = unbounded::<Message>();

        // Channel on which we receive our messages
        let (receiver_tx, receiver_rx) = unbounded::<Message>();

        let channels = ServerChannels {
            sender: receiver_tx,
            receiver: sender_rx,
        };

        channels_tx.send(channels)?;

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
        Ok(server)
    }

    pub fn start(&mut self) -> Result<(), Box<dyn Error>> {
        info!("{:?}: Started server", self.id);

        loop {
            // TODO: Add sender id to message
            select! {
                recv(self.receiver) -> message => {
                    let message = message?;
                    info!("{:?}: Received message: {:?}", self.id, message);
                },
                default(Duration::from_millis(100)) => {
                    trace!("{:?}: Timed out", self.id);
                },
            }

            match self.state {
                ServerState::Follower => {
                    // TODO: Wait for RPC request or response, or election timeout
                    thread::sleep(self.election_timeout);
                    info!("{:?}: Election timed out after {:?}", self.id, self.election_timeout);
                    self.start_election()?;
                }
                ServerState::Candidate => (),
                ServerState::Leader(_) => (),
            }
        }
    }

    fn start_election(&mut self) -> Result<(), Box<dyn Error>> {
        info!("{:?}: Becoming candidate", self.id);
        self.state = ServerState::Candidate;

        // Starting election:
        self.persistent.current_term += 1;

        // TODO: Vote for self

        self.reset_election_timeout();

        // TODO: Send RequestVote RPCs to all other servers
        self.sender.send(Message::RequestVoteRequest(
            RequestVoteArguments {
                term: self.persistent.current_term,
                candidate_id: self.id,
                last_log_index: LogIndex(0), // TODO
                last_log_term: Term(0), // TODO
            })
        )?;

        Ok(())
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
