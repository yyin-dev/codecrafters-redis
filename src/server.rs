use crate::mode::Mode;
use crate::store::Store;
use anyhow::{anyhow, Result};
use redis::{FromRedisValue, RedisResult, Value};
use redis_protocol::resp2::{
    decode::decode,
    encode::encode,
    types::{OwnedFrame, Resp2Frame},
};
use std::{
    io::{Read, Write},
    net::TcpStream,
    sync::{Arc, Mutex},
    time::Duration,
};

fn to_bulk_string_array(strs: Vec<String>) -> OwnedFrame {
    OwnedFrame::Array(
        strs.into_iter()
            .map(|s| OwnedFrame::BulkString(s.into()))
            .collect(),
    )
}

mod message {
    use redis_protocol::resp2::types::OwnedFrame;

    use super::to_bulk_string_array;

    pub fn ok() -> OwnedFrame {
        OwnedFrame::SimpleString("OK".into())
    }

    pub fn pong() -> OwnedFrame {
        OwnedFrame::SimpleString("PONG".into())
    }

    pub fn psync() -> OwnedFrame {
        to_bulk_string_array(vec!["PSYNC".into(), "?".into(), "-1".into()])
    }
}

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

fn write_bulk_string_array(stream: &mut TcpStream, strs: Vec<String>) -> Result<()> {
    write_frame(stream, to_bulk_string_array(strs))
}

fn receive(stream: &mut TcpStream) -> Result<Option<OwnedFrame>> {
    let mut buf = [0; 1024];
    let num_bytes = stream.read(&mut buf).unwrap();
    match decode(&buf[..num_bytes]).unwrap() {
        None => Ok(None),
        Some((frame, n)) => {
            assert!(n > 0);
            Ok(Some(frame))
        }
    }
}

fn expect(stream: &mut TcpStream, expected: OwnedFrame) -> Result<()> {
    let received = receive(stream)?.unwrap();

    if received != expected {
        return Err(anyhow!(
            "Expecting {:?}, received: {:?}",
            expected,
            received
        ));
    }

    Ok(())
}

impl Server {
    pub fn new(mode: Mode, port: u16) -> Result<Self> {
        let svr = Self {
            mode: mode.clone(),
            replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".into(),
            replication_offset: 0,
            store: Arc::new(Mutex::new(Store::new())),
        };

        // If it's a slave, handshake with master
        if let Mode::Slave(master_addr) = mode {
            let mut master_stream = TcpStream::connect(master_addr)?;

            // PING
            write_frame(
                &mut master_stream,
                OwnedFrame::Array(vec![OwnedFrame::BulkString("PING".into())]),
            )?;
            expect(&mut master_stream, message::pong())?;

            // REPLCONF
            write_bulk_string_array(
                &mut master_stream,
                vec!["REPLCONF".into(), "listening-port".into(), port.to_string()],
            )?;
            expect(&mut master_stream, message::ok())?;

            write_bulk_string_array(
                &mut master_stream,
                vec!["REPLCONF".into(), "capa".into(), "psync2".into()].into(),
            )?;
            expect(&mut master_stream, message::ok())?;

            // PSYNC
            write_frame(&mut master_stream, message::psync())?;
            let resp = receive(&mut master_stream)?.unwrap();
            println!("Recv: {:?}", resp);
        }

        Ok(svr)
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
                        write_frame(stream, message::ok())?
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
                    "replconf" => write_frame(stream, message::ok())?,
                    "psync" => {
                        write_frame(stream, OwnedFrame::BulkString("FULLRESYNC <REPL_ID> 0".into()))?
                    }
                    command => panic!("unknown command: {}", command),
                }
            }

            redis::Value::Status(_) => todo!(),
            redis::Value::Okay => todo!(),
        };

        Ok(())
    }
}
