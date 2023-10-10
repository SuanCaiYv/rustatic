use std::{sync::mpsc, thread, time::Duration};

use tracing::Level;

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

fn main() {
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
    let pool = pool::ThreadPool::new(2, 3, 1);
    _ = pool.execute(
        || {
            thread::sleep(Duration::from_secs(3));
            println!("hello1");
        },
        || {},
    );
    _ = pool.execute(
        || {
            thread::sleep(Duration::from_secs(3));
            println!("hello2");
        },
        || {},
    );
    _ = pool.execute(
        || {
            thread::sleep(Duration::from_secs(3));
            println!("hello3");
        },
        || {},
    );
    // _ = pool.execute(
    //     || {
    //         thread::sleep(Duration::from_secs(3));
    //         println!("hello4");
    //     },
    //     || {},
    // );
    // _ = pool.execute(
    //     || {
    //         thread::sleep(Duration::from_secs(3));
    //         println!("hello5");
    //     },
    //     || {},
    // );
    // _ = pool.execute(
    //     || {
    //         thread::sleep(Duration::from_secs(3));
    //         println!("hello6");
    //     },
    //     || {},
    // );
    // _ = pool.execute(
    //     || {
    //         thread::sleep(Duration::from_secs(3));
    //         println!("hello7");
    //     },
    //     || {},
    // );
    let (_tx, rx) = mpsc::channel::<()>();
    _ = rx.recv();
    // core_affinity::set_for_current(core_affinity::CoreId { id: 0 });
    // let socket = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
    // let bind_addr: SocketAddr = "0.0.0.0:8190".parse().unwrap();
    // let bind_addr = bind_addr.into();
    // socket.bind(&bind_addr).unwrap();
    // socket.listen(128).unwrap();
    // let listener: TcpListener = socket.into();
    // loop {
    //     let (stream, _) = listener.accept().unwrap();
    //     pool.execute(move || {
    //         handle(stream);
    //     }, Some(|| {}));
    // }
}

// 40ms
// fn handle(stream: TcpStream) {
//     core_affinity::set_for_current(core_affinity::CoreId { id: 1 });
//     let file = OpenOptions::new()
//         .read(true)
//         .open("/Users/joker/Downloads/VSCode-darwin-arm64.zip")
//         .unwrap();
//     let size = file.metadata().unwrap().len() as i64;
//     let (res, size) = nix::sys::sendfile::sendfile(file.as_fd(), stream.as_fd(), 0, Some(size), None, None);
//     println!("res: {:?}, size: {:?}", res, size);
// }

// 60ms
// fn handle1(mut stream: TcpStream) {
//     core_affinity::set_for_current(core_affinity::CoreId { id: 1 });
//     let mut file = OpenOptions::new()
//         .read(true)
//         .open("/Users/joker/Downloads/VSCode-darwin-arm64.zip")
//         .unwrap();
//     let size = file.metadata().unwrap().len() as usize;
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
//     let size = file.metadata().unwrap().len() as usize;
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
//     let size = file.metadata().unwrap().len() as usize;
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
