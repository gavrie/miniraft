use super::state::*;

// RPC

#[derive(Debug)]
pub enum Message {
    // Invoked by candidates to gather votes (§5.2)
    RequestVoteRequest(RequestVoteArguments),
    RequestVoteResponse(RequestVoteResults),
    AppendEntriesRequest(AppendEntriesArguments),
    AppendEntriesResponse(AppendEntriesResults),
}

#[derive(Debug)]
pub struct RequestVoteArguments {
    // Candidate’s term
    term: Term,

    // Candidate requesting vote
    candidate_id: ServerId,

    // Index of candidate’s last log entry (§5.4)
    last_log_index: LogIndex,

    // Term of candidate’s last log entry (§5.4)
    last_log_term: Term,
}

impl RequestVoteArguments {
    pub fn new(term: Term, candidate_id: ServerId) -> Self {
        Self {
            term,
            candidate_id,
            last_log_index: LogIndex(0), // TODO
            last_log_term: Term(0),      // TODO
        }
    }
}

#[derive(Debug)]
pub struct RequestVoteResults {
    // current_term, for candidate to update itself
    term: Term,

    // true means candidate received vote
    vote_granted: bool,
}

#[derive(Debug)]
pub struct AppendEntriesArguments {
    // Leader's term
    term: Term,

    // So follower can redirect clients
    leader_id: ServerId,

    // Index of log entry immediately preceding new ones
    prev_log_index: LogIndex,

    // Term of prev_log_index entry
    prev_log_term: Term,

    // Log entries to store (empty for heartbeat; may send more than one for efficiency)
    entries: Vec<LogEntry>,

    // Leader's commit_index
    leader_commit: LogIndex,
}

#[derive(Debug)]
pub struct AppendEntriesResults {
    // current_term, for leader to update itself
    term: Term,

    // True if follower contained entry matching prev_log_index and prev_log_term
    success: bool,
}
