use std::collections::HashMap;
use std::thread;
use std::sync::mpsc;

use super::state::*;
use super::server::{Server, ServerChannels};

pub struct Cluster {
    server_channels: HashMap<ServerId, ServerChannels>,
}

impl Cluster {
    pub fn new(num_servers: u32) -> Cluster {
        let mut server_channels = HashMap::new();

        for id in 1..=num_servers {
            let server_id = ServerId(id);
            let (tx, rx) = mpsc::channel();

            thread::spawn(move || {
                let mut server = Server::new(server_id, tx);
                server.start();
            });

            let channels = rx.recv().unwrap();
            server_channels.insert(server_id, channels);
        }

        Cluster { server_channels }
    }

    pub fn start(&self) {
        // Receive and send messages: Select on all channels and dispatch messages
    }
}
