use anyhow::Result;
use redis::{FromRedisValue, Value};
use redis_protocol::resp2::{encode::encode, types::OwnedFrame, types::Resp2Frame};
use std::{
    io::Write,
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

fn write_frame(stream: &mut TcpStream, frame: OwnedFrame) -> Result<()> {
    let mut buf = vec![0; frame.encode_len()];
    encode(&mut buf, &frame)?;
    stream.write_all(&buf)?;
    Ok(())
}

fn handle_client(mut stream: TcpStream) -> Result<()> {
    println!("Connection established");

    let mut parser = redis::Parser::new();
    loop {
        let result = parser.parse_value(&stream);

        match result {
            Ok(value) => handle_redis_value(&mut stream, value)?,
            Err(error) => {
                println!("parse error, will drop connection: {:?}", error.category());
                break;
            }
        }
    }

    Ok(())
}

fn handle_redis_value(stream: &mut TcpStream, value: Value) -> Result<()> {
    match value {
        redis::Value::Nil => todo!(),
        redis::Value::Int(_) => todo!(),
        redis::Value::Data(data) => {
            println!("Data: {:?}", data);
        }
        redis::Value::Bulk(values) => {
            println!("Bulk: {:?}", values);
            match &values[0] {
                redis::Value::Data(command) => {
                    match String::from_utf8_lossy(command)
                        .to_ascii_lowercase()
                        .as_str()
                    {
                        "ping" => write_frame(stream, OwnedFrame::BulkString("PONG".into()))?,
                        "echo" => {
                            let strings: Vec<String> = values
                                .into_iter()
                                .skip(1)
                                .map(|value| String::from_owned_redis_value(value).unwrap())
                                .collect::<Vec<String>>();

                            let owned_frames: Vec<OwnedFrame> = strings
                                .into_iter()
                                .map(|s| OwnedFrame::BulkString(s.into()))
                                .collect();

                            write_frame(stream, OwnedFrame::Array(owned_frames))?
                        }
                        command => panic!("unknown command: {}", command),
                    }
                }
                _ => todo!(),
            }
        }
        redis::Value::Status(_) => todo!(),
        redis::Value::Okay => todo!(),
    };

    Ok(())
}
