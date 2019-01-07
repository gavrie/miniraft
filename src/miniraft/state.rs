use core::fmt;
use std::ops::AddAssign;

#[derive(Debug)]
pub struct LogEntry {
    // Command for state machine
    command: Command,

    // Term when entry was received by leader (first index is 1)
    term: Term,
}

#[derive(Copy, Clone)]
pub struct LogIndex(pub u32);

impl fmt::Debug for LogIndex {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LogIndex({})", self.0)
    }
}

// Command for state machine
#[derive(Debug)]
struct Command;

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct Term(pub u32);

impl fmt::Debug for Term {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Term({})", self.0)
    }
}

impl AddAssign<u32> for Term {
    fn add_assign(&mut self, other: u32) {
        let Term(current) = *self;
        *self = Term(current + other)
    }
}

#[derive(Debug)]
pub struct CandidateId(u32);

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub struct ServerId(pub u32);

impl fmt::Debug for ServerId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ServerId({})", self.0)
    }
}
