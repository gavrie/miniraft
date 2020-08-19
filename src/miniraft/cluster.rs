use async_std::sync::{self, Receiver};
use log::info;
use std::collections::HashMap;
use std::sync::Arc;

use super::rpc::Message;
use super::server::Server;
use super::state::*;

pub struct Cluster {
    servers: HashMap<ServerId, Server>,
    rx: Receiver<(ServerId, Arc<Message>)>,
}

impl Cluster {
    pub async fn new(num_servers: u32) -> Result<Cluster> {
        let (tx, rx) = sync::channel(1);

        let servers: HashMap<_, _> = (1..=num_servers)
            .map(|id| {
                let server_id = ServerId(id);
                let server = Server::new(server_id, tx.clone());
                (server_id, server)
            })
            .collect();

        Ok(Cluster { servers, rx })
    }

    pub async fn run(&self) -> Result<()> {
        loop {
            let (sender_id, message) = self.rx.recv().await?;
            info!("<<< {:?}: {:?}", sender_id, message);

            // Broadcast the message to all servers
            for (_id, server) in self.servers.iter() {
                // FIXME: Don't send to originating server?
                let message = Arc::clone(&message);
                server.server_tx.send(message).await;
            }
        }
    }
}
