use std::error::Error;
use std::thread;
use std::time::Duration;

#[macro_use]
extern crate crossbeam_channel;

use env_logger;
use log::info;

mod miniraft;

use self::miniraft::cluster::Cluster;

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let num_servers = 3;
    info!("Creating a cluster with {} servers", num_servers);
    let cluster = Cluster::new(num_servers)?;
    cluster.start()?;

    loop {
        thread::sleep(Duration::from_secs(60 * 60 * 24));
    }
}
