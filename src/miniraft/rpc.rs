use super::state::*;
use std::sync::Arc;

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
pub enum Target {
    All,
    Server(ServerId),
}

pub struct FramedMessage {
    pub source: ServerId,
    pub target: Target,
    pub message: Arc<Message>,
}

#[derive(Debug)]
pub struct RequestVoteArguments {
    // Candidate’s term
    pub term: Term,

    // Candidate requesting vote
    pub candidate_id: ServerId,

    // Index of candidate’s last log entry (§5.4)
    pub last_log_index: LogIndex,

    // Term of candidate’s last log entry (§5.4)
    pub last_log_term: Term,
}

#[derive(Debug)]
pub struct RequestVoteResults {
    // current_term, for candidate to update itself
    pub term: Term,

    // true means candidate received vote
    pub vote_granted: bool,
}

#[derive(Debug)]
pub struct AppendEntriesArguments {
    // Leader's term
    pub term: Term,

    // So follower can redirect clients
    pub leader_id: ServerId,

    // Index of log entry immediately preceding new ones
    pub prev_log_index: LogIndex,

    // Term of prev_log_index entry
    pub prev_log_term: Term,

    // Log entries to store (empty for heartbeat; may send more than one for efficiency)
    pub entries: Vec<LogEntry>,

    // Leader's commit_index
    pub leader_commit: LogIndex,
}

#[derive(Debug)]
pub struct AppendEntriesResults {
    // current_term, for leader to update itself
    pub term: Term,

    // True if follower contained entry matching prev_log_index and prev_log_term
    pub success: bool,
}
