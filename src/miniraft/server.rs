use super::state::*;

#[derive(Debug)]
pub struct Server {
    pub id: ServerId,
    state: ServerState,
    persistent: PersistentData,
    volatile: VolatileData,
}

impl Server {
    pub fn new(id: ServerId) -> Self {
        Self {
            id,
            state: ServerState::Follower,
            persistent: PersistentData::new(),
            volatile: VolatileData::new(),
        }
    }

    pub fn start(&self) {
        println!("Started server {:?}", self.id);
    }
}
