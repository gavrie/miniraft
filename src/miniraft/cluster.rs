use std::collections::HashMap;
use std::error::Error;
use std::thread;

use crossbeam_channel::{unbounded, Select};
use crossbeam_channel::Receiver;
use log::info;

use super::state::*;
use super::server::{Server, ServerChannels};
use super::rpc::Message;

pub struct Cluster {
    server_channels: HashMap<ServerId, ServerChannels>,
}

impl Cluster {
    pub fn new(num_servers: u32) -> Result<Cluster, Box<dyn Error>> {
        let mut server_channels = HashMap::new();

        for id in 1..=num_servers {
            let server_id = ServerId(id);
            let (tx, rx) = unbounded();

            thread::spawn(move || {
                let mut server = Server::new(server_id, tx).unwrap();
                server.start().unwrap();
            });

            let channels = rx.recv()?;
            server_channels.insert(server_id, channels);
        }

        Ok(Cluster { server_channels })
    }

    pub fn start(&self) -> Result<(), Box<dyn Error>> {
        // Receive and send messages: Select on all channels and dispatch messages.

        let receivers: Vec<_> = self.server_channels
            .iter()
            .map(|(&id, ServerChannels { receiver, sender: _ })| (id, receiver))
            .collect();

        let senders: Vec<_> = self.server_channels
            .iter()
            .map(|(_, ServerChannels { receiver: _, sender })| sender)
            .collect();

        loop {
            let (sender_id, message) = Self::receive(&receivers)?;
            info!("Received message from {:?}: {:?}", sender_id, message);

            // Broadcast the message to all servers
            for &s in senders.iter() {
                let message = message.clone();
                s.send(message)?;
            }
        }
    }

    fn receive(receivers: &[(ServerId, &Receiver<Message>)])
               -> Result<(ServerId, Message), Box<dyn Error>> {
        let mut sel = Select::new();

        for (_, rx) in receivers {
            sel.recv(rx);
        }

        let oper = sel.select();
        let (server_id, rx) = receivers[oper.index()];
        let message = oper.recv(rx)?;

        Ok((server_id, message))
    }
}
