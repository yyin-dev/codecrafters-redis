use crate::message;
use crate::store::Store;
use anyhow::Result;
use base64::Engine;
use redis::{FromRedisValue, RedisResult, Value};
use redis_protocol::resp2::{
    encode::encode,
    types::{OwnedFrame, Resp2Frame},
};
use std::{
    io::Write,
    net::TcpStream,
    sync::{Arc, Mutex},
    time::Duration,
};

pub struct Master {
    replication_id: String,
    replication_offset: usize,
    store: Arc<Mutex<Store>>,
    replicas: Arc<Mutex<Vec<TcpStream>>>,
}

fn write_frame(stream: &mut TcpStream, frame: OwnedFrame) -> Result<()> {
    let mut buf = vec![0; frame.encode_len()];
    encode(&mut buf, &frame)?;
    stream.write_all(&buf)?;
    Ok(())
}
fn to_bulk_string_array(strs: Vec<String>) -> OwnedFrame {
    OwnedFrame::Array(
        strs.into_iter()
            .map(|s| OwnedFrame::BulkString(s.into()))
            .collect(),
    )
}
fn write_bulk_string_array(stream: &mut TcpStream, strs: Vec<String>) -> Result<()> {
    write_frame(stream, to_bulk_string_array(strs))
}
impl Master {
    pub fn new() -> Result<Self> {
        let master = Self {
            replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".into(),
            replication_offset: 0,
            store: Arc::new(Mutex::new(Store::new())),
            replicas: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(master)
    }

    pub fn handle_connection(&self, mut stream: TcpStream) -> Result<()> {
        let mut parser = redis::Parser::new();

        let mut is_replica = false;
        while !is_replica {
            let result = parser.parse_value(&stream);

            match result {
                Err(error) => {
                    println!("Error: {:?}, will close connection", error.category());
                    break;
                }
                Ok(value) => {
                    is_replica = self.handle_value(&mut stream, value)?;
                }
            }
        }

        if is_replica {
            // TODO: Call handle_replica() to handle commands like ping, replconf, psync, etc.
            self.replicas.lock().unwrap().push(stream);
        }

        Ok(())
    }

    // Return true if this connection is from a replica (b/c we just completed a handshake)
    fn handle_value(&self, stream: &mut TcpStream, value: Value) -> Result<bool> {
        let mut is_replica = false;

        match value {
            redis::Value::Nil => todo!(),
            redis::Value::Int(_) => todo!(),
            redis::Value::Data(_) => todo!(),
            redis::Value::Status(_) => todo!(),
            redis::Value::Okay => todo!(),
            redis::Value::Bulk(values) => {
                println!("Bulk: {:?}", values);

                let string_from = |idx: usize| -> RedisResult<String> {
                    String::from_owned_redis_value(values.get(idx).unwrap().clone())
                };

                match string_from(0)?.to_ascii_lowercase().as_str() {
                    "ping" => write_frame(stream, message::pong())?,
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
                        write_frame(stream, message::ok())?;

                        // Replications
                        let mut replicas = self.replicas.lock().unwrap();
                        let replication_cmd: Vec<String> = values
                            .iter()
                            .map(|value| String::from_owned_redis_value(value.clone()).unwrap())
                            .collect();
                        println!("Replication cmd: {:?}", replication_cmd);
                        replicas
                            .iter_mut()
                            .map(|replica_stream| {
                                write_bulk_string_array(replica_stream, replication_cmd.clone())
                            })
                            .collect::<Result<Vec<()>>>()?;
                    }
                    "info" => match string_from(1)?.to_ascii_lowercase().as_str() {
                        "replication" => {
                            let role = String::from("role:master");
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
                    "replconf" => write_frame(stream, message::ok())?,
                    "psync" => {
                        let slave_replication_id = string_from(1)?;
                        let slave_replication_offset: isize = string_from(2)?.parse()?;

                        if slave_replication_id == "?" {
                            assert_eq!(slave_replication_offset, -1);
                            write_frame(
                                stream,
                                OwnedFrame::BulkString(
                                    format!("FULLRESYNC {} 0", self.replication_id).into(),
                                ),
                            )?;

                            // Send RDB file. Assume empty for this challenge
                            // Format: $<length_of_file>\r\n<contents_of_file>
                            // Like bulk string, but without trailing \r\n
                            let empty_rdb_base64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
                            let empty_rdb = base64::engine::general_purpose::STANDARD
                                .decode(empty_rdb_base64)?;
                            let bulk_string = OwnedFrame::BulkString(empty_rdb);
                            let mut buf = vec![0; bulk_string.encode_len()];
                            encode(&mut buf, &bulk_string)?;
                            stream.write_all(&buf[..buf.len() - 2])?;
                            println!("Written rdb file");

                            is_replica = true;
                        } else {
                            todo!()
                        }
                    }
                    command => panic!("unknown command: {}", command),
                }
            }
        };

        Ok(is_replica)
    }
}
