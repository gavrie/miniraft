use async_std::sync::{self, Receiver};
use std::collections::HashMap;

use super::server::Server;
use super::state::*;
use crate::miniraft::rpc::{FramedMessage, Target};

pub struct Cluster {
    pub servers: HashMap<ServerId, Server>,
    rx: Receiver<FramedMessage>,
}

impl Cluster {
    pub async fn new(num_servers: u32) -> Result<Cluster> {
        let (tx, rx) = sync::channel(1);

        let server_ids: Vec<_> = (1..=num_servers).map(ServerId).collect();

        let servers: HashMap<_, _> = server_ids
            .iter()
            .map(|id| {
                let server = Server::new(id.clone(), server_ids.clone(), tx.clone());
                (id.clone(), server)
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
