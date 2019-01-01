// State

use core::fmt;

#[derive(Debug)]
enum ServerType {
    Follower,
    Candidate,
    Leader(VolatileStateLeader),
}

#[derive(Debug)]
pub struct State {
    persistent: PersistentState,
    volatile: VolatileState,
    server_type: ServerType,
}

// Persistent State on all servers.
// (Updated on stable storage before responding to RPCs)
#[derive(Debug)]
struct PersistentState {
    // Latest Term server has seen (initialized to 0 on first boot, increases monotonically)
    current_term: Term,

    // CandidateId that received vote in current term (or None)
    voted_for: Option<CandidateId>,

    // Log entries
    log: Vec<LogEntry>,
}

// Volatile State on all servers.
#[derive(Debug)]
struct VolatileState {
    // Index of highest log entry known to be committed (initialized to 0, increases monotonically)
    commit_index: LogIndex,

    // Index of highest log entry applied to state machine (initialized to 0, increases monotonically)
    last_applied: LogIndex,
}

// Volatile State on leaders (Reinitialized after election).
#[derive(Debug)]
struct VolatileStateLeader {
    // For each server, index of the next log entry to send to that server
    // (initialized to leader last log index + 1)
    next_indexes: Vec<LogIndex>,
    // For each server, index of highest log entry known to be replicated on server
    // (initialized to 0, increases monotonically)
    match_indexes: Vec<LogIndex>,

}

//////////////////////////////////////////////
//
// Helper types
//

#[derive(Debug)]
pub struct LogEntry {
    // Command for state machine
    command: Command,

    // Term when entry was received by leader (first index is 1)
    term: Term,
}

pub struct LogIndex(u32);

impl fmt::Debug for LogIndex {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LogIndex({})", self.0)
    }
}

// Command for state machine
#[derive(Debug)]
struct Command;

pub struct Term(u32);

impl fmt::Debug for Term {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Term({})", self.0)
    }
}

#[derive(Debug)]
pub struct CandidateId(u32);

#[derive(Debug)]
pub struct ServerId(u32);

//////////////////////////////////////////////
//
// Implementation
//

impl State {
    pub fn new() -> State {
        State {
            persistent: PersistentState {
                current_term: Term(0),
                voted_for: None,
                log: Vec::new(),
            },
            volatile: VolatileState {
                commit_index: LogIndex(0),
                last_applied: LogIndex(0),
            },
            server_type: ServerType::Follower,
        }
    }
}
