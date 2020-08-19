use async_std::sync::{self, Receiver, Sender};
use async_std::task::JoinHandle;
use log::info;
use std::collections::HashMap;
use std::sync::Arc;

use super::rpc::Message;
use super::server::Server;
use super::state::*;

struct ServerHandle {
    server_tx: Sender<Arc<Message>>,
    handle: JoinHandle<()>,
}

pub struct Cluster {
    server_handles: HashMap<ServerId, ServerHandle>,
    rx: Receiver<(ServerId, Arc<Message>)>,
}

impl Cluster {
    pub async fn new(num_servers: u32) -> Result<Cluster> {
        let (tx, rx) = sync::channel(1);

        let server_handles: HashMap<_, _> = (1..=num_servers)
            .map(|id| {
                let server_id = ServerId(id);

                let (server_tx, server_rx) = sync::channel(1);
                let handle = Server::spawn(server_id, tx.clone(), server_rx);

                (server_id, ServerHandle { server_tx, handle })
            })
            .collect();

        Ok(Cluster { server_handles, rx })
    }

    pub async fn run(&self) -> Result<()> {
        loop {
            let (sender_id, message) = self.rx.recv().await?;
            info!("<<< {:?}: {:?}", sender_id, message);

            // Broadcast the message to all servers
            for (_id, handle) in self.server_handles.iter() {
                let message = Arc::clone(&message);
                handle.server_tx.send(message).await;
            }
        }
    }
}
