use super::state::*;
use std::marker::PhantomData;

////////////////////
// RPC types

pub trait RPC {}


#[derive(Debug)]
struct AppendEntries;

impl RPC for AppendEntries {}

#[derive(Debug)]
pub struct RequestVote;

impl RPC for RequestVote {}

////////////////////
// Messages

trait MessageData {}

pub trait Arguments<R: RPC> {}

pub trait Results<R: RPC> {}


#[derive(Debug)]
pub struct Request<R: RPC, A: Arguments<R>> {
    arguments: A,
    rpc: PhantomData<R>,
}

impl<R: RPC, A: Arguments<R>> MessageData for Request<R, A> {}

impl Request<RequestVote, RequestVoteArguments> {
    pub fn new(term: Term, candidate_id: ServerId) -> Self {
        Self {
            arguments: RequestVoteArguments {
                term,
                candidate_id,
                last_log_index: LogIndex(0), // TODO
                last_log_term: Term(0),      // TODO
            },
            rpc: PhantomData
        }
    }
}


#[derive(Debug)]
struct Response<R: RPC, A: Results<R>> {
    results: A,
    rpc: PhantomData<R>,
}

impl<R: RPC, A: Results<R>> MessageData for Response<R, A> {}

////////////////////

#[derive(Debug)]
pub enum Message {
    // Invoked by candidates to gather votes (§5.2)
    RequestVoteRequest(Request<RequestVote, RequestVoteArguments>),
    RequestVoteResponse(Response<RequestVote, RequestVoteResults>),

    // ...
    AppendEntriesRequest(Request<AppendEntries, AppendEntriesArguments>),
    AppendEntriesResponse(Response<AppendEntries, AppendEntriesResults>),
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

impl Arguments<RequestVote> for RequestVoteArguments {}

#[derive(Debug)]
struct RequestVoteResults {
    // current_term, for candidate to update itself
    term: Term,

    // true means candidate received vote
    vote_granted: bool,
}

impl Results<RequestVote> for RequestVoteResults {}

#[derive(Debug)]
struct AppendEntriesArguments {
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

impl Arguments<AppendEntries> for AppendEntriesArguments {}

#[derive(Debug)]
struct AppendEntriesResults {
    // current_term, for leader to update itself
    term: Term,

    // True if follower contained entry matching prev_log_index and prev_log_term
    success: bool,
}

impl Results<AppendEntries> for AppendEntriesResults {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc() {
        let r = Request {
            arguments: AppendEntriesArguments {
                term: Term(0),
                leader_id: ServerId(0),
                prev_log_index: LogIndex(0),
                prev_log_term: Term(0),
                entries: vec![],
                leader_commit: LogIndex(0)
            },
            rpc: PhantomData,
        };
    }
}