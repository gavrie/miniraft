// State

#[derive(Debug)]
pub struct State {
    persistent: PersistentState,
    volatile: VolatileState,
    volatile_leader: Option<VolatileStateLeader>,
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
struct LogEntry {
    // Command for state machine
    command: Command,

    // Term when entry was received by leader (first index is 1)
    term: Term,
}

#[derive(Debug)]
pub struct LogIndex(u32);

// Command for state machine
#[derive(Debug)]
struct Command;

#[derive(Debug)]
pub struct Term(u32);

#[derive(Debug)]
pub struct CandidateId(u32);

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
            volatile_leader: Option::from(VolatileStateLeader {
                next_indexes: Vec::new(),
                match_indexes: Vec::new(),
            }),
        }
    }
}
