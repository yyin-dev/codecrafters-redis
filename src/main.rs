mod server;
mod store;
use clap::Parser;
use std::{
    net::{Ipv4Addr, SocketAddrV4, TcpListener},
    sync::Arc,
    thread,
};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(long)]
    port: Option<String>,
}

fn main() {
    let cli = Cli::parse();

    let port = match cli.port {
        None => 6379,
        Some(port) => {
            let port: u16 = port.parse().unwrap();
            port
        }
    };
    let sockaddr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port);
    println!("Addr: {}", sockaddr);

    let server = Arc::new(server::Server::new());
    let listener = TcpListener::bind(sockaddr).unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let server = server.clone();
                thread::spawn(move || server.handle_new_client(stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
