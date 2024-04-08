mod server;
mod store;

use std::{net::TcpListener, sync::Arc, thread};

fn main() {
    let server = Arc::new(server::Server::new());

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
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
