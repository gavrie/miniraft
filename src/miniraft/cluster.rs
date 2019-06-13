use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::thread;

use crossbeam_channel::{unbounded, Select};
use crossbeam_channel::{Receiver, Sender};
use log::info;

use super::rpc::Message;
use super::server::Server;
use super::state::*;

pub struct Cluster {
    server_senders: HashMap<ServerId, Sender<Arc<Message>>>,
    server_receivers: HashMap<ServerId, Receiver<Arc<Message>>>,
}

impl Cluster {
    pub fn new(num_servers: u32) -> Result<Cluster, Box<dyn Error>> {
        let mut server_senders = HashMap::new();
        let mut server_receivers = HashMap::new();

        for id in 1..=num_servers {
            let server_id = ServerId(id);
            let (tx, rx) = unbounded();

            thread::spawn(move || {
                let mut server = Server::new(server_id, tx).unwrap();
                server.start().unwrap();
            });

            let stub_message = rx.recv()?;

            server_senders.insert(server_id, stub_message.sender);
            server_receivers.insert(server_id, stub_message.receiver);
        }

        Ok(Cluster {
            server_senders,
            server_receivers,
        })
    }

    pub fn start(&self) -> Result<(), Box<dyn Error>> {
        // Receive and send messages: Select on all channels and dispatch messages.

        loop {
            let (sender_id, message) = self.receive()?;
            info!("Received message from {:?}: {:?}", sender_id, message);

            // Broadcast the message to all servers
            for (_id, sender) in self.server_senders.iter() {
                let message = Arc::clone(&message);
                sender.send(message)?;
            }
        }
    }

    fn receive(&self) -> Result<(ServerId, Arc<Message>), Box<dyn Error>> {
        let mut sel = Select::new();

        let mut receivers = vec![];
        for (&server_id, rx) in self.server_receivers.iter() {
            receivers.push((server_id, rx));
            sel.recv(rx);
        }

        let oper = sel.select();
        let (server_id, rx) = receivers[oper.index()];
        let message = oper.recv(rx)?;

        Ok((server_id, message))
    }
}
