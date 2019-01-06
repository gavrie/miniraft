use std::thread;
use std::time::Duration;

use super::state::*;
use super::server::Server;

#[derive(Debug)]
pub struct Cluster;

impl Cluster {
    pub fn new(num_servers: u32) -> Cluster {
        for id in 1..=num_servers {
            let server = Server::new(ServerId(id));
            println!("Starting thread for {:?}", server.id);
            thread::spawn(move || {
                server.start();
            });
        }

        Cluster {}
    }
}
