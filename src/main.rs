use async_std::task;
use env_logger;
use env_logger::Env;
use log::info;
use std::time::Duration;

mod miniraft;

use self::miniraft::cluster::Cluster;
use self::miniraft::state::Result;

async fn async_main() -> Result<()> {
    let num_servers = 3;
    info!("Creating a cluster with {} servers", num_servers);
    let cluster = Cluster::new(num_servers).await?;
    cluster.run().await?;

    loop {
        task::sleep(Duration::from_secs(60 * 60 * 24)).await;
    }
}

fn main() -> Result<()> {
    env_logger::init_from_env(Env::default().default_filter_or("info"));
    task::block_on(async_main())
}
