use std::env;

use net::server::{Server, ServerConfig};
use tracing::Level;
use structopt::StructOpt;

mod db;
mod net;

#[derive(StructOpt, Debug)]
#[structopt(name = "rustatic")]
struct Opt {
    #[structopt(short = "r", long, default_value = "")]
    /// the file root folder
    root: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();
    tracing_subscriber::fmt()
        .event_format(
            tracing_subscriber::fmt::format()
                .with_line_number(true)
                .with_level(true)
                .with_target(true),
        )
        .with_max_level(Level::INFO)
        .try_init()
        .unwrap();
    let server_config = ServerConfig {
        ctrl_port: 8190,
        data_port: 8191,
        cert: rustls::Certificate(include_bytes!("../tls/dev-server.crt.der").to_vec()),
        key: rustls::PrivateKey(include_bytes!("../tls/dev-server.key.der").to_vec()),
        root_folder: if opt.root.len() == 0 {
            format!(
                "{}/rustatic/data",
                env::var("HOME").expect("HOME env var not set")
            )
        } else {
            opt.root.clone()
        },
    };
    println!("{}", server_config.root_folder);
    Server::new(server_config).run().await
}
