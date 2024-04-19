#[allow(dead_code)]
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

#[allow(dead_code)]
fn receive_raw(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut buf = [0; 1024];
    let num_bytes = stream.read(&mut buf)?;
    println!("{} bytes read", num_bytes);
    Ok(buf[..num_bytes].to_vec())
}

fn skip_rdb_file(stream: &mut TcpStream) -> Result<()> {
    println!("Parsing rdb file response");

    // Format: $<length_of_file>\r\n<contents_of_file>
    // Like bulk string, but without trailing \r\n
    let mut one_char_buf: [u8; 1] = [0];
    stream.read_exact(&mut one_char_buf)?;
    match one_char_buf.get(0).unwrap().clone() as char {
        '$' => println!("Read $"),
        '*' => println!("Read *"),
        c => panic!("Read '{}'", c),
    };

    let mut digits = String::new();
    loop {
        stream.read_exact(&mut one_char_buf)?;
        let c = one_char_buf.get(0).unwrap().clone() as char;
        if char::is_numeric(c) {
            digits.push(c);
            println!("Read {}", c);
        } else {
            assert_eq!(c, '\r');

            stream.read_exact(&mut one_char_buf)?;
            assert_eq!(one_char_buf.get(0).unwrap().clone() as char, '\n');

            break;
        }
    }

    let length: usize = digits.parse()?;
    println!("length: {}", length);

    let mut buf = vec![0; length];
    stream.read_exact(&mut buf)?;

    println!("Successfully read rdb file");
    Ok(())
}

fn receive(stream: &mut TcpStream) -> Result<Option<OwnedFrame>> {
    let mut buf = [0; 1024];
    let num_bytes = stream.read(&mut buf)?;
    match decode(&buf[..num_bytes])? {
        None => Ok(None),
        Some((frame, n)) => {
            assert!(n > 0);
            assert_eq!(num_bytes, n);
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
        match skip_rdb_file(&mut master_stream) {
            Ok(()) => println!("Skipped rdb file"),
            Err(err) => println!("Failed to skip rdb file, err: {}", err),
        };

        println!("Finished handshaking!");
        let replica = Arc::new(Self {
            master_replication_id: master_replication_id.into(),
            replication_offset: 0,
            store: Arc::new(Mutex::new(Store::new())),
        });

        let replica_clone = replica.clone();
        thread::spawn(move || replica_clone.handle_replication(master_stream));

        // Sadly, the sleep is required to pass replication-13, to give the
        // replication-handling thread enough time to process replication cmds.
        // Otherwise, the thread doesn't have a chance to wake up and process
        // replication cmds when the client keeps sending GET queries. I tried
        // thread::yield_now and thread::sleep in the client-handling thread,
        // but neither works.
        thread::sleep(Duration::from_secs(1));
        Ok(replica)
    }

    fn handle_replication(&self, mut master_stream: TcpStream) -> Result<()> {
        println!("Start handling replication cmds...");
        loop {
            println!("Waiting for next replication cmd");
            match receive(&mut master_stream)? {
                None => {
                    println!("No more message, will close connection");
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
        println!("Start handing queries...");

        let mut buf = [0; 1024];
        loop {
            let num_bytes = stream.read(&mut buf)?;
            let result = redis::parse_redis_value(&buf[..num_bytes]);

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
                    command => println!("unknown command: {}", command),
                }
            }

            redis::Value::Status(_) => todo!(),
            redis::Value::Okay => todo!(),
        };

        Ok(())
    }
}
