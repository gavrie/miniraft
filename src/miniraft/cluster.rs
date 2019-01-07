use std::thread;

use log::info;

use super::state::*;
use super::server::Server;

#[derive(Debug)]
pub struct Cluster;

impl Cluster {
    pub fn new(num_servers: u32) -> Cluster {
        for id in 1..=num_servers {
            let server_id = ServerId(id);
            info!("Starting thread for {:?}", server_id);
            thread::spawn(move || {
                let mut server = Server::new(server_id);
                server.start();
            });
        }

        Cluster {}
    }
}
