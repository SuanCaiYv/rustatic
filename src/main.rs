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
    #[structopt(short = "l", long, default_value = "info")]
    log: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();
    let log_level = match opt.log.as_str() {
        "info" | "i" => {
            Level::INFO
        },
        "warn" | "w" => {
            Level::WARN
        },
        "debug" | "d" => {
            Level::DEBUG
        }
        "error" | "e" => {
            Level::ERROR
        }
        _ => {
            Level::INFO
        },
    };
    tracing_subscriber::fmt()
        .event_format(
            tracing_subscriber::fmt::format()
                .with_line_number(true)
                .with_level(true)
                .with_target(true),
        )
        .with_max_level(log_level)
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
    println!("Save file in 👉{}👈", server_config.root_folder);
    Server::new(server_config).run().await
}
