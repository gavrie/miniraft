mod miniraft;

use self::miniraft::state::Cluster;

fn setup() {
    let cluster = Cluster::new();
    println!("Cluster: {:#?}", cluster);
}

fn main() {
    setup()
}
