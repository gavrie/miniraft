use std::collections::HashMap;
use std::thread;

use crossbeam_channel::{unbounded, Select};
use log::info;

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
            let (tx, rx) = unbounded();

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
        // Receive and send messages: Select on all channels and dispatch messages.

        let mut receivers = Vec::new();
        let mut sel = Select::new();
        for (server_id,
            ServerChannels {
                receiver: rx,
                sender: _,
            }) in self.server_channels.iter() {
            receivers.push((server_id, rx));
            sel.recv(rx);
        }

        let oper = sel.select();
        let index = oper.index();
        let (server_id, rx) = receivers[index];
        let message = oper.recv(rx).unwrap();

        info!("Received message from {:?}: {:?}", server_id, message);
    }
}
