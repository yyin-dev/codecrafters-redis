use crate::connection::Connection;
use crate::data::{self, Data};
use crate::store::Store;
use anyhow::anyhow;
use anyhow::Result;
use base64::Engine;
use std::{
    net::TcpStream,
    sync::{Arc, Mutex},
    time::Duration,
};

pub struct Master {
    replication_id: String,
    replication_offset: usize,
    store: Arc<Mutex<Store>>,
    replicas: Arc<Mutex<Vec<Connection>>>,
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

    pub fn handle_connection(&self, stream: TcpStream) -> Result<()> {
        let mut conn = Connection::new(stream);

        let mut is_replica = false;
        while !is_replica {
            let result = conn.read_data();

            match result {
                Err(error) => {
                    println!("Error: {:?}, will close connection", error);
                    break;
                }
                Ok(data) => {
                    is_replica = self.handle_data(&mut conn, data)?;
                }
            }
        }

        if is_replica {
            // TODO: Call handle_replica() to handle commands like ping, replconf, psync, etc.
            self.replicas.lock().unwrap().push(conn);
        }

        Ok(())
    }

    // Return true if this connection is from a replica (b/c we just completed a handshake)
    fn handle_data(&self, conn: &mut Connection, data: Data) -> Result<bool> {
        let mut is_replica = false;

        match data {
            Data::Array(vs) => {
                println!("Bulk: {:?}", vs);

                let string_from = |idx| -> Result<String> {
                    let data: &Data = vs.get(idx).unwrap();
                    match data.to_string() {
                        None => Err(anyhow!("to_string failed")),
                        Some(s) => Ok(s),
                    }
                };

                match string_from(0)?.to_ascii_lowercase().as_str() {
                    "ping" => conn.write_data(Data::SimpleString("PONG".into()))?,
                    "echo" => {
                        assert_eq!(vs.len(), 2);
                        let string = string_from(1)?;
                        conn.write_data(Data::BulkString(string.into()))?
                    }
                    "get" => {
                        let store = self.store.lock().unwrap();

                        assert_eq!(vs.len(), 2);
                        let key = string_from(1)?;
                        match store.get(&key) {
                            None => conn.write_data(Data::NullBulkString)?,
                            Some(value) => conn.write_data(Data::BulkString(value.into()))?,
                        }
                    }
                    "set" => {
                        let store = self.store.lock().unwrap();

                        assert!(vs.len() == 3 || vs.len() == 5);
                        let key = string_from(1)?;
                        let value = string_from(2)?;

                        let expire_in = if vs.len() == 5 {
                            let px = string_from(3)?;
                            assert_eq!(px.to_ascii_lowercase(), "px");
                            let expire_in: u64 = string_from(4)?.parse()?;
                            Some(Duration::from_millis(expire_in))
                        } else {
                            None
                        };

                        store.set(key, value, expire_in);
                        conn.write_data(Data::SimpleString("OK".into()))?;

                        // Replications
                        let mut replicas = self.replicas.lock().unwrap();
                        replicas
                            .iter_mut()
                            .map(|replica_conn| replica_conn.write_data(Data::Array(vs.clone())))
                            .collect::<Result<Vec<()>>>()?;
                    }
                    "info" => match string_from(1)?.to_ascii_lowercase().as_str() {
                        "replication" => {
                            let role = String::from("role:master");
                            let replication_id = format!("master_replid:{}", self.replication_id);
                            let replication_offset =
                                format!("master_repl_offset:{}", self.replication_offset);

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
                        let slave_replication_id = string_from(1)?;
                        let slave_replication_offset: isize = string_from(2)?.parse()?;

                        if slave_replication_id == "?" {
                            assert_eq!(slave_replication_offset, -1);
                            conn.write_data(Data::SimpleString(
                                format!("FULLRESYNC {} 0", self.replication_id).into(),
                            ))?;

                            // Send RDB file. Assume empty for this challenge
                            // Format: $<length_of_file>\r\n<contents_of_file>
                            // Like bulk string, but without trailing \r\n
                            let empty_rdb_base64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
                            let empty_rdb = base64::engine::general_purpose::STANDARD
                                .decode(empty_rdb_base64)?;
                            conn.write(data::encode_rdb_file(empty_rdb))?;

                            is_replica = true;
                        } else {
                            todo!()
                        }
                    }
                    command => panic!("unknown command: {}", command),
                }
            }
            v => println!("Unkonwn: {:?}", v),
        };

        Ok(is_replica)
    }
}
