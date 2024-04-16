use crate::message;
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
    net::{SocketAddr, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

fn to_bulk_string_array(strs: Vec<String>) -> OwnedFrame {
    OwnedFrame::Array(
        strs.into_iter()
            .map(|s| OwnedFrame::BulkString(s.into()))
            .collect(),
    )
}

fn write_frame(stream: &mut TcpStream, frame: OwnedFrame) -> Result<()> {
    let mut buf = vec![0; frame.encode_len()];
    encode(&mut buf, &frame)?;
    stream.write_all(&buf)?;
    Ok(())
}

fn write_null_bulk_string(stream: &mut TcpStream) -> Result<()> {
    let buf = "$-1\r\n".as_bytes();
    stream.write_all(&buf)?;
    Ok(())
}

fn write_bulk_string_array(stream: &mut TcpStream, strs: Vec<String>) -> Result<()> {
    write_frame(stream, to_bulk_string_array(strs))
}

fn receive_raw(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut buf = [0; 1024];
    let num_bytes = stream.read(&mut buf)?;
    println!("{} bytes read", num_bytes);
    Ok(buf[..num_bytes].to_vec())
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

fn receive_string(stream: &mut TcpStream) -> Result<Option<String>> {
    let frame = receive(stream)?;

    if let Some(frame) = frame {
        match frame {
            OwnedFrame::SimpleString(s) | OwnedFrame::BulkString(s) => {
                Ok(Some(String::from_utf8(s)?))
            }
            _ => Err(anyhow!("Not a string")),
        }
    } else {
        Ok(None)
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

fn print_frame(frame: &OwnedFrame) {
    match frame {
        OwnedFrame::SimpleString(s) => {
            let s = String::from_utf8(s.clone()).unwrap();
            println!("SimpleString: '{}'", s)
        }
        OwnedFrame::Error(_) => todo!(),
        OwnedFrame::Integer(_) => todo!(),
        OwnedFrame::BulkString(s) => {
            let s = String::from_utf8(s.clone()).unwrap();
            println!("BulkString: '{}'", s)
        }
        OwnedFrame::Array(vs) => {
            let s: Vec<String> = vs.iter().map(|f| f.to_string().unwrap()).collect();
            println!("Array: {:?}", s);
        }
        OwnedFrame::Null => todo!(),
    }
}

pub struct Replica {
    master_replication_id: String,
    replication_offset: usize,
    store: Arc<Mutex<Store>>,
}

impl Replica {
    pub fn new(master_sockaddr: SocketAddr, port: u16) -> Result<Arc<Self>> {
        // If it's a slave, handshake with master
        let mut master_stream = TcpStream::connect(master_sockaddr)?;

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
        let fullresync_resp = receive_string(&mut master_stream)?.unwrap();
        let master_replication_id = fullresync_resp.split_ascii_whitespace().nth(1).unwrap();
        println!("Master replication id: {}", master_replication_id);
        // Format: $<length_of_file>\r\n<contents_of_file>
        // Like bulk string, but without trailing \r\n
        // Convert to bulk string and parse
        let mut rdb_file_resp = receive_raw(&mut master_stream)?;
        if rdb_file_resp.len() == 0 {
            println!("Received an empty rdb file transfer");
        } else {
            println!("Received a non-empty rdb file transfer");

            rdb_file_resp.push('\r' as u8);
            rdb_file_resp.push('\n' as u8);
            let (as_bulk_string, num_bytes) = decode(&rdb_file_resp)?.unwrap();
            assert_eq!(num_bytes, rdb_file_resp.len());

            if let OwnedFrame::BulkString(v) = as_bulk_string {
                println!("Rdb file: {} bytes", v.len());
            } else {
                panic!("Fail to parse rdb file response");
            }
        }

        println!("Finished handshaking!");
        let replica = Arc::new(Self {
            master_replication_id: master_replication_id.into(),
            replication_offset: 0,
            store: Arc::new(Mutex::new(Store::new())),
        });

        let replica_clone = replica.clone();
        thread::spawn(move || replica_clone.handle_replication(master_stream));

        Ok(replica)
    }

    fn handle_replication(&self, mut master_stream: TcpStream) -> Result<()> {
        loop {
            match receive(&mut master_stream)? {
                None => {
                    println!("No more messages, will close connection");
                    break;
                }
                Some(frame) => {
                    print_frame(&frame);
                    self.handle_propogation(frame)?;
                }
            }
        }

        Ok(())
    }

    fn handle_propogation(&self, frame: OwnedFrame) -> Result<()> {
        match frame {
            OwnedFrame::SimpleString(_) => todo!(),
            OwnedFrame::Error(_) => todo!(),
            OwnedFrame::Integer(_) => todo!(),
            OwnedFrame::BulkString(_) => todo!(),
            OwnedFrame::Array(values) => {
                let string_from = |idx| -> Result<String> {
                    let frame: &OwnedFrame = values.get(idx).unwrap();
                    match frame.to_string() {
                        None => Err(anyhow!("to_string failed")),
                        Some(s) => Ok(s),
                    }
                };

                match string_from(0)?.to_ascii_lowercase().as_str() {
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
                    }
                    command => panic!("unknown command: {}", command),
                }
            }
            OwnedFrame::Null => todo!(),
        }

        Ok(())
    }

    pub fn handle_connection(&self, mut stream: TcpStream) -> Result<()> {
        let mut parser = redis::Parser::new();
        loop {
            let result = parser.parse_value(&stream);

            match result {
                Ok(value) => self.handle_value(&mut stream, value)?,
                Err(error) => {
                    println!("Error: {:?}, will close connection", error.category());
                    break;
                }
            }
        }

        Ok(())
    }

    fn handle_value(&self, stream: &mut TcpStream, value: Value) -> Result<()> {
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
                            None => write_null_bulk_string(stream)?,
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
                            let role = String::from("role:slave");
                            let replication_id =
                                format!("master_replid:{}", self.master_replication_id);
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
