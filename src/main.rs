mod store;

use anyhow::Result;
use redis::{FromRedisValue, RedisResult, Value};
use redis_protocol::resp2::{encode::encode, types::OwnedFrame, types::Resp2Frame};
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    sync::Arc,
    thread,
    time::Duration,
};
use store::Store;

fn main() {
    let store = Arc::new(Store::new());

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let store = store.clone();
                thread::spawn(move || handle_client(stream, store).unwrap());
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

fn handle_client(mut stream: TcpStream, store: Arc<Store>) -> Result<()> {
    println!("Connection established");

    let mut parser = redis::Parser::new();
    loop {
        let result = parser.parse_value(&stream);

        match result {
            Ok(value) => handle_redis_value(&mut stream, store.as_ref(), value)?,
            Err(error) => {
                println!("parse error, will drop connection: {:?}", error.category());
                break;
            }
        }
    }

    Ok(())
}

fn handle_redis_value(stream: &mut TcpStream, store: &Store, value: Value) -> Result<()> {
    match value {
        redis::Value::Nil => todo!(),
        redis::Value::Int(_) => todo!(),
        redis::Value::Data(data) => {
            println!("Data: {:?}", data);
        }
        redis::Value::Bulk(values) => {
            println!("Bulk: {:?}", values);

            let string_from = |idx: usize| -> RedisResult<String> {
                String::from_owned_redis_value(values.get(idx).unwrap().clone())
            };

            match &values[0] {
                redis::Value::Data(command) => {
                    match String::from_utf8_lossy(command)
                        .to_ascii_lowercase()
                        .as_str()
                    {
                        "ping" => write_frame(stream, OwnedFrame::BulkString("PONG".into()))?,
                        "echo" => {
                            assert_eq!(values.len(), 2);
                            let string = string_from(1)?;
                            write_frame(stream, OwnedFrame::BulkString(string.into()))?
                        }
                        "get" => {
                            assert_eq!(values.len(), 2);
                            let key = string_from(1)?;
                            match store.get(&key) {
                                None => write_frame(stream, OwnedFrame::Null)?,
                                Some(value) => {
                                    write_frame(stream, OwnedFrame::BulkString(value.into()))?
                                }
                            }
                        }
                        "set" => {
                            assert!(values.len() == 3 || values.len() == 5);
                            let key = string_from(1)?;
                            let value = string_from(2)?;

                            let expire_in = if values.len() == 5 {
                                let px = string_from(3)?;
                                assert_eq!(px.to_ascii_lowercase(), "px");
                                let expire_in: u64 = string_from(4)?.parse()?;
                                Some(Duration::from_millis(expire_in))
                            } else {
                                None
                            };

                            store.set(key, value, expire_in);
                            write_frame(stream, OwnedFrame::SimpleString("OK".into()))?
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
