mod mode;
mod server;
mod store;
use clap::Parser;
use mode::Mode;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, TcpListener},
    str::FromStr,
    sync::Arc,
    thread,
};

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(long)]
    port: Option<u16>,
    #[arg(long = "replicaof", value_names = &["MASTER_HOST", "MASTER_PORT"], num_args = 2)]
    replica_of: Option<Vec<String>>,
}

fn main() {
    let cli = Cli::parse();
    println!("{:?}", cli);

    let mode = match &cli.replica_of {
        None => Mode::Master,
        Some(args) => {
            assert_eq!(args.len(), 2);
            let addr = if args.get(0).unwrap() == "localhost" {
                IpAddr::from_str("127.0.0.1").unwrap()
            } else {
                IpAddr::from_str(args.get(0).unwrap()).unwrap()
            };
            let port: u16 = args.get(1).unwrap().clone().parse().unwrap();
            Mode::Slave(SocketAddr::new(addr, port))
        }
    };
    println!("mode: {:?}", mode);

    let port = cli.port.unwrap_or(6379);
    let sockaddr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port);

    let server = Arc::new(server::Server::new(mode, port).unwrap());
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
