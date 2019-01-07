mod miniraft;

use self::miniraft::cluster::Cluster;
use std::thread;
use std::time::Duration;

use env_logger;
use log::info;

fn main() {
    env_logger::init();

    let num_servers = 3;
    info!("Creating a cluster with {} servers", num_servers);
    let _cluster = Cluster::new(num_servers);

    loop {
        thread::sleep(Duration::from_secs(60 * 60 * 24));
    }
}
