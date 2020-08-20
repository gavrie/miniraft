use async_std::task;
use env_logger;
use env_logger::Env;
use log::info;

mod miniraft;

use self::miniraft::cluster::Cluster;
use self::miniraft::state::Result;

async fn async_main() -> Result<()> {
    let num_servers = 3;
    info!("Creating a cluster with {} servers", num_servers);
    let cluster = Cluster::new(num_servers).await?;
    cluster.run().await?;

    for (_server_id, server) in cluster.servers {
        server.handle.await;
    }

    Ok(())
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("heartbeatd=info"))
        .format_timestamp_millis()
        .init();

    task::block_on(async_main())
}
