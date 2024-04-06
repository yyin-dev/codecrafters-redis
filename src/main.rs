use anyhow::Result;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => handle_client(stream).unwrap(),
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream) -> Result<()> {
    println!("Connection established");

    loop {
        // Cannot use read_to_string - probably because the bytes are not utf-8
        let mut buf = [0; 128];
        let bytes_read = stream.read(&mut buf)?;

        if bytes_read == 0 {
            continue;
        }

        println!("recv: {:?}", buf);
        stream.write_all("+PONG\r\n".as_bytes())?
    }
}
