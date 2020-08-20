use std::error::Error;
use std::ops::AddAssign;

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Debug, Clone)]
pub struct LogEntry {
    // Command for state machine
    command: Command,

    // Term when entry was received by leader (first index is 1)
    term: Term,
}

#[derive(Copy, Clone, Debug)]
pub struct LogIndex(pub u32);

// Command for state machine
#[derive(Debug, Clone)]
struct Command;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd)]
pub struct Term(pub u32);

impl AddAssign<u32> for Term {
    fn add_assign(&mut self, other: u32) {
        let Term(current) = *self;
        *self = Term(current + other)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServerId(pub u32);
