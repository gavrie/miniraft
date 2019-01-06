mod miniraft;

use self::miniraft::cluster::Cluster;
use std::thread;
use std::time::Duration;

fn main() {
    let num_servers = 3;
    println!("Creating a cluster with {} servers", num_servers);
    let cluster = Cluster::new(num_servers);

    loop {
        thread::sleep(Duration::from_secs(60*60*24));
    }
}
