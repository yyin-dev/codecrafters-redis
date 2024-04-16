mod master;
mod message;
mod mode;
mod replica;
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

    match mode {
        Mode::Master => {
            let master = Arc::new(master::Master::new().unwrap());
            let listener = TcpListener::bind(sockaddr).unwrap();
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let master = master.clone();
                        thread::spawn(move || master.handle_connection(stream));
                    }
                    Err(e) => {
                        println!("error: {}", e);
                    }
                }
            }
        }
        Mode::Slave(master_sockaddr) => {
            let listener = TcpListener::bind(sockaddr).unwrap();
            let replica = replica::Replica::new(master_sockaddr, port).unwrap();
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let replica = replica.clone();
                        thread::spawn(move || replica.handle_connection(stream));
                    }
                    Err(e) => {
                        println!("error: {}", e);
                    }
                }
            }
        }
    }
}
