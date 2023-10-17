use net::server::{ServerConfig, Server};
use tracing::Level;

mod db;
mod net;
mod pool;

// #[tokio::main]
// async fn main() {
//     let listener = tokio::net::TcpListener::bind("0.0.0.0:8190").await.unwrap();
//     loop {
//         let (stream, _) = listener.accept().await.unwrap();
//         tokio::spawn(async move {
//             handle4(stream).await;
//         });
//     }
// }

// #[tokio::main]
// async fn main() -> anyhow::Result<()> {
//     tracing_subscriber::fmt()
//         .event_format(
//             tracing_subscriber::fmt::format()
//                 .with_line_number(true)
//                 .with_level(true)
//                 .with_target(true),
//         )
//         .with_max_level(Level::INFO)
//         .try_init()
//         .unwrap();
//
//     let server_config = ServerConfig {
//         ctrl_port: 8190,
//         data_port: 8191,
//         cert: rustls::Certificate(include_bytes!("/Users/joker/RustProjects/prim/server/cert/localhost-server.crt.der").to_vec()),
//         key: rustls::PrivateKey(include_bytes!("/Users/joker/RustProjects/prim/server/cert/localhost-server.key.der").to_vec()),
//     };
//     Server::new(server_config).run().await?;
//     Ok(())
// }

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
        cert: rustls::Certificate(include_bytes!(
            "/Users/slma/RustProjects/prim/server/cert/localhost-server.crt.der"
        ).to_vec()),
        key: rustls::PrivateKey(include_bytes!(
            "/Users/slma/RustProjects/prim/server/cert/localhost-server.key.der"
        ).to_vec()),
    };
    Server::new(server_config).run().await
}

// 40ms
// fn handle(stream: TcpStream) {
//     core_affinity::set_for_current(core_affinity::CoreId { id: 1 });
//     let file = OpenOptions::new()
//         .read(true)
//         .open("/Users/joker/Downloads/Adobe_Lightroom_Classic_2023_v12_4.dmg")
//         .unwrap();
//     let mut size = file.metadata().unwrap().len() as i64;
//     println!("size: {}", size);
//     let file_fd = file.as_fd();
//     let mut offset = 0;
//     loop {
//         let t = timestamp();
//         let (res, n) = sendfile::sendfile(file_fd, stream.as_fd(), offset, Some(size), None, None);
//         if res.is_err() {
//             error!("download error: {} {}", res.unwrap_err(), n);
//         }
//         if size == 0 {
//             break;
//         }
//         offset += n;
//         size -= n;
//         println!("time: {} remain: {}", (timestamp() - t) / 1000, size);
//     }
// }

// 60ms
// fn handle1(mut stream: TcpStream) {
//     core_affinity::set_for_current(core_affinity::CoreId { id: 1 });
//     let mut file = OpenOptions::new()
//         .read(true)
//         .open("/Users/joker/Downloads/VSCode-darwin-arm64.zip")
//         .unwrap();
//     let size = file.db().unwrap().len() as usize;
//     let mut buffer = vec![0; 1024 * 1024 * 8];
//     let mut total = 0;
//     loop {
//         let res = file.read(&mut buffer).unwrap();
//         if res == 0 {
//             break;
//         }
//         let res = stream.write(&buffer[..res]).unwrap();
//         total += res;
//         if total == size {
//             break;
//         }
//     }
// }

// 50ms
// fn handle2(mut stream: TcpStream) {
//     core_affinity::set_for_current(core_affinity::CoreId { id: 1 });
//     let mut file = OpenOptions::new()
//         .read(true)
//         .open("/Users/joker/Downloads/VSCode-darwin-arm64.zip")
//         .unwrap();
//     let size = file.db().unwrap().len() as usize;
//     let map = unsafe {
//         mmap(None, NonZeroUsize::new_unchecked(size), ProtFlags::PROT_READ, MapFlags::MAP_PRIVATE, Some(file.as_fd()), 0).unwrap()
//     };
//     unsafe {
//         mprotect(map, size, ProtFlags::PROT_READ).unwrap();
//     }
//     unsafe {
//         let map = map as *const u8;
//         let slice: &[u8] = std::slice::from_raw_parts(map, size);
//         stream.write_all(slice).unwrap();
//     }
//     stream.flush().unwrap();
//     unsafe {
//         munmap(map, size).unwrap();
//     }
// }

// fn handle3(stream: TcpStream) {
//     core_affinity::set_for_current(core_affinity::CoreId { id: 1 });
//     let mut file = OpenOptions::new()
//         .read(true)
//         .open("/Users/slma/Downloads/Teams_osx.pkg")
//         .unwrap();
//     let size = file.db().unwrap().len() as usize;
//
//     let (pipe_rd, pipe_wr) = pipe().unwrap();
//     loop {
//         let res = splice(file.as_fd(), None, pipe_wr, None, size, nix::fcntl::SpliceFFlags::empty()).unwrap();
//         if res == 0 {
//             break;
//         }
//         let res = splice(pipe_rd, None, stream.as_fd(), None, size, nix::fcntl::SpliceFFlags::empty()).unwrap();
//         if res == 0 {
//             break;
//         }
//     }
// }

// 50ms
// async fn handle4(mut stream: tokio::net::TcpStream) {
//     let mut file = tokio::fs::OpenOptions::new()
//         .read(true)
//         .open("/Users/slma/Downloads/Teams_osx.pkg")
//         .await
//         .unwrap();
//     let mut buffer = vec![0; 1024 * 1024 * 16];
//     loop {
//         let res = file.read(&mut buffer).await.unwrap();
//         if res == 0 {
//             break;
//         }
//         let res = stream.write(&buffer[..res]).await.unwrap();
//         if res == 0 {
//             break;
//         }
//     }
// }
