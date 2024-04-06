use anyhow::Result;
use std::{
    net::{TcpListener, TcpStream},
    thread,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(|| handle_client(stream).unwrap());
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_client(stream: TcpStream) -> Result<()> {
    println!("Connection established");

    let mut parser = redis::Parser::new();
    loop {
        let result = parser.parse_value(&stream);

        match result {
            Err(error) => {
                println!("parse error, will drop connection: {:?}", error.category());
                break;
            }
            Ok(value) => match value {
                redis::Value::Nil => todo!(),
                redis::Value::Int(_) => todo!(),
                redis::Value::Data(data) => {
                    println!("Data: {:?}", data);
                }
                redis::Value::Bulk(values) => {
                    println!("Bulk: {:?}", values);
                }
                redis::Value::Status(_) => todo!(),
                redis::Value::Okay => todo!(),
            },
        }
    }

    Ok(())
}
