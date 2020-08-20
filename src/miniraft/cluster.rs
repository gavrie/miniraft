use async_std::sync::{self, Receiver};
use log::info;
use std::collections::HashMap;
use std::sync::Arc;

use super::rpc::Message;
use super::server::Server;
use super::state::*;
use crate::miniraft::rpc::{FramedMessage, Target};

pub struct Cluster {
    servers: HashMap<ServerId, Server>,
    rx: Receiver<FramedMessage>,
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
            let frame = self.rx.recv().await?;
            // info!(
            //     "<<< {:?}->{:?}: {:?}",
            //     frame.source, frame.target, frame.message
            // );

            match frame.target {
                Target::Server(server_id) => {
                    let server = self.servers.get(&server_id).unwrap();
                    server.server_tx.send(frame).await;
                }

                Target::All => {
                    // Broadcast the message to all servers
                    for (server_id, server) in self.servers.iter() {
                        if server_id == &frame.source {
                            continue;
                        }

                        server
                            .server_tx
                            .send(FramedMessage {
                                source: frame.source,
                                target: Target::All,
                                message: frame.message.clone(),
                            })
                            .await;
                    }
                }
            }
        }
    }
}
