use crate::mode::Mode;
use crate::store::Store;
use redis::{FromRedisValue, RedisResult, Value};
use redis_protocol::{
    resp2::{
        encode::encode,
        types::{OwnedFrame, Resp2Frame},
    },
    types::PATTERN_PUBSUB_PREFIX,
};
use std::{
    fmt::format,
    io::Write,
    net::TcpStream,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Result;

pub struct Server {
    mode: Mode,
    replication_id: String,
    replication_offset: usize,
    store: Arc<Mutex<Store>>,
}

fn write_frame(stream: &mut TcpStream, frame: OwnedFrame) -> Result<()> {
    let mut buf = vec![0; frame.encode_len()];
    encode(&mut buf, &frame)?;
    stream.write_all(&buf)?;
    Ok(())
}

impl Server {
    pub fn new(mode: Mode) -> Self {
        Self {
            mode,
            replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".into(),
            replication_offset: 0,
            store: Arc::new(Mutex::new(Store::new())),
        }
    }

    pub fn handle_new_client(&self, mut stream: TcpStream) -> Result<()> {
        let mut parser = redis::Parser::new();
        loop {
            let result = parser.parse_value(&stream);

            match result {
                Ok(value) => self.handle_redis_value(&mut stream, value)?,
                Err(error) => {
                    println!("Error: {:?}, will close connection", error.category());
                    break;
                }
            }
        }

        Ok(())
    }

    fn handle_redis_value(&self, stream: &mut TcpStream, value: Value) -> Result<()> {
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

                match string_from(0)?.to_ascii_lowercase().as_str() {
                    "ping" => write_frame(stream, OwnedFrame::BulkString("PONG".into()))?,
                    "echo" => {
                        assert_eq!(values.len(), 2);
                        let string = string_from(1)?;
                        write_frame(stream, OwnedFrame::BulkString(string.into()))?
                    }
                    "get" => {
                        let store = self.store.lock().unwrap();

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
                        let store = self.store.lock().unwrap();

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
                    "info" => match string_from(1)?.to_ascii_lowercase().as_str() {
                        "replication" => {
                            let role = {
                                let role = match self.mode {
                                    Mode::Master => "master",
                                    Mode::Slave(_) => "slave",
                                };
                                format!("role:{}", role)
                            };

                            let replication_id = format!("master_replid:{}", self.replication_id);
                            let replication_offset =
                                format!("master_repl_offset:{}", self.replication_offset);

                            write_frame(
                                stream,
                                OwnedFrame::BulkString(
                                    vec![role, replication_id, replication_offset]
                                        .join("\n")
                                        .into(),
                                ),
                            )?
                        }
                        info_type => panic!("unknown info type: {}", info_type),
                    },
                    command => panic!("unknown command: {}", command),
                }
            }

            redis::Value::Status(_) => todo!(),
            redis::Value::Okay => todo!(),
        };

        Ok(())
    }
}
