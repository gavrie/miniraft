use std::error::Error;
use std::ops::{Add, AddAssign};

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Debug, Clone)]
pub struct LogEntry {
    // Command for state machine
    pub command: Command,

    // Term when entry was received by leader (first index is 1)
    pub term: Term,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LogIndex(usize);

impl LogIndex {
    pub const ZERO: LogIndex = Self(0);

    pub fn new(index: usize) -> Self {
        Self(index)
    }

    pub fn zero_based(&self) -> usize {
        if self.0 == 0 {
            panic!("Tried to access invalid index 0");
        } else {
            self.0 - 1
        }
    }
}

impl Add<usize> for LogIndex {
    type Output = LogIndex;

    fn add(self, rhs: usize) -> Self::Output {
        let LogIndex(current) = self;
        LogIndex(current + rhs)
    }
}

impl AddAssign<usize> for LogIndex {
    fn add_assign(&mut self, rhs: usize) {
        let Self(current) = *self;
        *self = Self(current + rhs)
    }
}

// Command for state machine
#[derive(Debug, Clone)]
pub struct Command(pub String);

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
