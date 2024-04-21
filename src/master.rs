use crate::connection::Connection;
use crate::data::{self, Data};
use crate::store::Store;
use anyhow::anyhow;
use anyhow::Result;
use base64::Engine;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::mpsc;
use std::{
    net::TcpStream,
    sync::{Arc, Mutex},
    time::Duration,
};

struct ReplicaHandle {
    id: usize,
    conn: Connection,
}

pub struct MasterInner {
    replication_id: String,
    replication_offset: usize,
    store: Store,
    replicas: Vec<Arc<ReplicaHandle>>,
}

pub struct Master {
    inner: Arc<Mutex<MasterInner>>,
}

impl Master {
    pub fn new() -> Result<Self> {
        let inner = MasterInner {
            replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".into(),
            replication_offset: 0,
            store: Store::new(),
            replicas: Vec::new(),
        };
        let master = Self {
            inner: Arc::new(Mutex::new(inner)),
        };

        Ok(master)
    }

    pub fn handle_connection(&self, stream: TcpStream) -> Result<()> {
        let mut conn = Connection::new(stream);

        loop {
            let result = conn.read_data();

            match result {
                Err(error) => {
                    println!("Error: {:?}, will close connection", error);
                    break;
                }
                Ok(data) => {
                    let is_replica = self.handle_data(&mut conn, data)?;
                    if is_replica {
                        let mut inner = self.inner.lock().unwrap();

                        let handle = ReplicaHandle {
                            id: inner.replicas.len(),
                            conn,
                        };
                        let handle = Arc::new(handle);

                        inner.replicas.push(handle.clone());
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    // Return true if this connection is from a replica (b/c we just completed a handshake)
    fn handle_data(&self, conn: &mut Connection, data: Data) -> Result<bool> {
        println!("Recv: {}", data);
        let num_bytes = data.num_bytes();
        match data {
            Data::Array(vs) => {
                let string_at = |idx: usize| -> Result<String> {
                    vs[idx].get_string().ok_or(anyhow!("fail to get string"))
                };

                match string_at(0)?.to_ascii_lowercase().as_str() {
                    "ping" => conn.write_data(Data::SimpleString("PONG".into()))?,
                    "echo" => {
                        assert_eq!(vs.len(), 2);
                        let string = string_at(1)?;
                        conn.write_data(Data::BulkString(string.into()))?
                    }
                    "get" => {
                        let inner = self.inner.lock().unwrap();

                        assert_eq!(vs.len(), 2);
                        let key = string_at(1)?;
                        match inner.store.get(&key) {
                            None => conn.write_data(Data::NullBulkString)?,
                            Some(value) => conn.write_data(Data::BulkString(value.into()))?,
                        }
                    }
                    "set" => {
                        let mut inner = self.inner.lock().unwrap();

                        assert!(vs.len() == 3 || vs.len() == 5);
                        let key = string_at(1)?;
                        let value = string_at(2)?;

                        let expire_in = if vs.len() == 5 {
                            let px = string_at(3)?;
                            assert_eq!(px.to_ascii_lowercase(), "px");
                            let expire_in: u64 = string_at(4)?.parse()?;
                            Some(Duration::from_millis(expire_in))
                        } else {
                            None
                        };

                        inner.store.set(key, value, expire_in);
                        conn.write_data(Data::SimpleString("OK".into()))?;

                        // Replications
                        inner
                            .replicas
                            .iter_mut()
                            .map(|replica| replica.conn.write_data(Data::Array(vs.clone())))
                            .collect::<Result<Vec<()>>>()?;

                        inner.replication_offset += num_bytes;
                        println!("replication offset: +{}", inner.replication_offset);
                    }
                    "info" => match string_at(1)?.to_ascii_lowercase().as_str() {
                        "replication" => {
                            let inner = self.inner.lock().unwrap();
                            let role = String::from("role:master");
                            let replication_id = format!("master_replid:{}", inner.replication_id);
                            let replication_offset =
                                format!("master_repl_offset:{}", inner.replication_offset);

                            conn.write_data(Data::BulkString(
                                vec![role, replication_id, replication_offset]
                                    .join("\n")
                                    .into(),
                            ))?
                        }
                        info_type => panic!("unknown info type: {}", info_type),
                    },
                    "replconf" => conn.write_data(Data::SimpleString("OK".into()))?,
                    "psync" => {
                        let slave_replication_id = string_at(1)?;
                        let slave_replication_offset: isize = string_at(2)?.parse()?;

                        if slave_replication_id == "?" {
                            assert_eq!(slave_replication_offset, -1);
                            conn.write_data(Data::SimpleString(
                                format!(
                                    "FULLRESYNC {} 0",
                                    self.inner.lock().unwrap().replication_id
                                )
                                .into(),
                            ))?;

                            // Send RDB file. Assume empty for this challenge
                            // Format: $<length_of_file>\r\n<contents_of_file>
                            // Like bulk string, but without trailing \r\n
                            let empty_rdb_base64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
                            let empty_rdb = base64::engine::general_purpose::STANDARD
                                .decode(empty_rdb_base64)?;
                            conn.write(data::encode_rdb_file(empty_rdb))?;

                            println!("Finished handshaking with replica");
                            return Ok(true);
                        } else {
                            todo!()
                        }
                    }
                    "wait" => {
                        assert_eq!(vs.len(), 3);

                        let mut inner = self.inner.lock().unwrap();

                        let num_replicas_to_wait = string_at(1)?.parse::<usize>()?;

                        if num_replicas_to_wait > 0 && inner.replication_offset > 0 {
                            println!("Sending getack to replicas...");
                            let getack = Data::Array(vec![
                                Data::BulkString("REPLCONF".into()),
                                Data::BulkString("GETACK".into()),
                                Data::BulkString("*".into()),
                            ]);
                            for r in inner.replicas.iter() {
                                r.conn.write_data(getack.clone())?;
                            }

                            println!("Waiting acks from replicas...");

                            let cnt = {
                                let timeout = Duration::from_millis(string_at(2)?.parse()?);
                                let (tx, rx) = mpsc::channel();
                                let replication_offset = inner.replication_offset;
                                let cnt = Arc::new(AtomicUsize::new(0));
                                for r in inner.replicas.iter() {
                                    let r = r.clone();
                                    let cnt = cnt.clone();
                                    let tx = tx.clone();
                                    std::thread::spawn(move || -> Result<()> {
                                        let data = r.conn.read_data()?;
                                        if let Data::Array(vs) = data {
                                            let string_at = |idx: usize| -> Result<String> {
                                                vs[idx]
                                                    .get_string()
                                                    .ok_or(anyhow!("fail to get string"))
                                            };

                                            match string_at(0)?.to_ascii_uppercase().as_str() {
                                                "REPLCONF" => {
                                                    assert_eq!(vs.len(), 3);
                                                    assert_eq!(string_at(1)?, "ACK");
                                                    let offset = string_at(2)?.parse::<usize>()?;
                                                    println!(
                                                        "replica {}: {}. Replication offset: {}",
                                                        r.id, offset, replication_offset
                                                    );
                                                    if offset == replication_offset {
                                                        cnt.fetch_update(SeqCst, SeqCst, |c| {
                                                            if c + 1 == num_replicas_to_wait {
                                                                match tx.send(()) {
                                                                    Ok(()) => (),
                                                                    Err(_) => (),
                                                                };
                                                            }
                                                            Some(c + 1)
                                                        })
                                                        .unwrap();
                                                    };
                                                    Ok(())
                                                }
                                                _ => unreachable!(),
                                            }
                                        } else {
                                            unreachable!()
                                        }
                                    });
                                }

                                if let Err(err) = rx.recv_timeout(timeout) {
                                    println!("Timeout: {}", err);
                                };
                                cnt.load(SeqCst)
                            };
                            println!("cnt: {}", cnt);

                            inner.replication_offset += getack.num_bytes();
                            println!("replication offset: +{}", getack.num_bytes());
                            conn.write_data(Data::Integer(cnt as i64))?
                        } else {
                            conn.write_data(Data::Integer(inner.replicas.len() as i64))?
                        }
                    }
                    command => panic!("unknown command: {}", command),
                }
            }
            v => println!("Unkonwn: {:?}", v),
        };

        Ok(false)
    }
}
