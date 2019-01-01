use super::state::*;

// RPC

enum Request {
    // Invoked by candidates to gather votes (§5.2)
    RequestVote(RequestVoteArguments),

    AppendEntries(AppendEntriesArguments),
}

enum Response {
    RequestVote(RequestVoteResults),
    AppendEntries(AppendEntriesResults),
}

struct RequestVoteArguments {
    // Candidate’s term
    term: Term,

    // Candidate requesting vote
    candidate_id: CandidateId,

    // Index of candidate’s last log entry (§5.4)
    last_log_index: LogIndex,

    // Term of candidate’s last log entry (§5.4)
    last_log_term: Term,
}

struct RequestVoteResults {
    // currentTerm, for candidate to update itself
    term: Term,

    // true means candidate received vote
    vote_granted: bool,
}

struct AppendEntriesArguments {}

struct AppendEntriesResults {}
