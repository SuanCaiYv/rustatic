use std::env;
use std::time::Instant;

use net::server::{Server, ServerConfig};
use tracing::Level;
use crate::pool::ThreadPool;

mod db;
mod net;
mod pool;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let n: u64 = 10000;
    let pool1 = threadpool::Builder::new().num_threads(4).build();
    let pool2: ThreadPool<u64> = pool::ThreadPool::new(1, 4, 8);
    let t = Instant::now();
    for i in 0..n {
        pool1.execute(move || {
            let _ = i;
        });
    }
    pool1.join();
    println!("time: {:?}", t.elapsed());
    Ok(())
    // tracing_subscriber::fmt()
    //     .event_format(
    //         tracing_subscriber::fmt::format()
    //             .with_line_number(true)
    //             .with_level(true)
    //             .with_target(true),
    //     )
    //     .with_max_level(Level::INFO)
    //     .try_init()
    //     .unwrap();
    // let server_config = ServerConfig {
    //     ctrl_port: 8190,
    //     data_port: 8191,
    //     cert: rustls::Certificate(include_bytes!("../tls/dev-server.crt.der").to_vec()),
    //     key: rustls::PrivateKey(include_bytes!("../tls/dev-server.key.der").to_vec()),
    //     root_folder: format!(
    //         "{}/rustatic/data",
    //         env::var("HOME").expect("HOME env var not set")
    //     ),
    // };
    // Server::new(server_config).run().await
}
