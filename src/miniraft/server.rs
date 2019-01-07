use std::thread;
use std::time::Duration;

use log::info;
use rand::prelude::*;

use super::state::*;

#[derive(Debug)]
pub struct Server {
    pub id: ServerId,
    state: ServerState,
    persistent: PersistentData,
    volatile: VolatileData,
    rng: ThreadRng,
}

impl Server {
    pub fn new(id: ServerId) -> Self {
        Self {
            id,
            state: ServerState::Follower,
            persistent: PersistentData::new(),
            volatile: VolatileData::new(),
            rng: thread_rng(),
        }
    }

    pub fn start(&mut self) {
        info!("{:?}: Started server", self.id);

        loop {
            match self.state {
                ServerState::Follower => {
                    // Wait for RPC or election timeout
                    let election_timeout = self.rng.gen_range(150, 300);
                    let election_timeout = Duration::from_millis(election_timeout);

                    thread::sleep(election_timeout);
                    info!("{:?}: Election timed out after {:?}", self.id, election_timeout);
                    self.start_election();
                }
                ServerState::Candidate => (),
                ServerState::Leader(_) => (),
            }
        }
    }

    pub fn start_election(&mut self) {
        info!("{:?}: Becoming candidate", self.id);
        self.state = ServerState::Candidate;
    }
}
