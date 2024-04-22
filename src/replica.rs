use crate::connection::Connection;
use crate::data::Data;
use crate::store::Store;
use crate::value::Value;
use anyhow::{anyhow, Result};
use std::{
    net::{SocketAddr, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

pub struct Replica {
    master_replication_id: String,
    replication_offset: Arc<Mutex<usize>>,
    store: Arc<Mutex<Store>>,
}

impl Replica {
    pub fn new(master_sockaddr: SocketAddr, port: u16) -> Result<Arc<Self>> {
        // If it's a slave, handshake with master
        let master_stream = TcpStream::connect(master_sockaddr)?;
        let conn = Connection::new(master_stream);

        // PING
        conn.write_data(Data::Array(vec![Data::BulkString("PING".into())]))?;
        assert_eq!(conn.read_data()?, Data::SimpleString("PONG".into()));

        // REPLCONF
        conn.write_data(Data::Array(vec![
            Data::BulkString("REPLCONF".into()),
            Data::BulkString("listening-port".into()),
            Data::BulkString(port.to_string().into()),
        ]))?;
        assert_eq!(conn.read_data()?, Data::SimpleString("OK".into()));

        conn.write_data(Data::Array(vec![
            Data::BulkString("REPLCONF".into()),
            Data::BulkString("capa".into()),
            Data::BulkString("psync2".into()),
        ]))?;
        assert_eq!(conn.read_data()?, Data::SimpleString("OK".into()));

        // PSYNC
        conn.write_data(Data::Array(vec![
            Data::BulkString("PSYNC".into()),
            Data::BulkString("?".into()),
            Data::BulkString("-1".into()),
        ]))?;
        let resp = conn.read_data()?;
        let master_replication_id = if let Data::SimpleString(s) = resp {
            String::from_utf8(s)?
                .split_ascii_whitespace()
                .nth(1)
                .unwrap()
                .to_string()
        } else {
            panic!("Expect FULLRESYNC");
        };
        println!("Master replication id: {}", master_replication_id);
        let rdb_file = conn.read_rdb_file()?;
        println!("Rdb file is {} bytes long", rdb_file.len());

        println!("Finished handshaking!");
        let replica = Arc::new(Self {
            master_replication_id: master_replication_id.into(),
            replication_offset: Arc::new(Mutex::new(0)),
            store: Arc::new(Mutex::new(Store::new())),
        });

        let replica_clone = replica.clone();
        thread::spawn(move || replica_clone.handle_replication(conn));

        Ok(replica)
    }

    fn handle_replication(self: Arc<Self>, conn: Connection) -> Result<()> {
        println!("Start handling replication cmds...");
        let conn = Arc::new(conn);

        loop {
            let res = conn.read_data();

            if let Ok(data) = res {
                println!("Replication : {}", data);
                let cmd_len = data.num_bytes();
                match data {
                    Data::Array(vs) => {
                        let string_at = |idx: usize| -> Result<String> {
                            vs[idx].get_string().ok_or(anyhow!("fail to get string"))
                        };

                        match string_at(0)?.to_ascii_uppercase().as_str() {
                            "PING" => println!("Received PING from master"),
                            "SET" => {
                                let store = self.store.lock().unwrap();

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

                                store.set(key, Value::String(value), expire_in);
                            }
                            "REPLCONF" => {
                                assert_eq!(vs.len(), 3);
                                assert_eq!(string_at(1)?, "GETACK");
                                assert_eq!(string_at(2)?, "*");

                                conn.write_data(Data::Array(vec![
                                    Data::BulkString("REPLCONF".into()),
                                    Data::BulkString("ACK".into()),
                                    Data::BulkString(
                                        self.replication_offset.lock().unwrap().to_string().into(),
                                    ),
                                ]))?
                            }
                            command => panic!("unknown command: {}", command),
                        };

                        let mut offset = self.replication_offset.lock().unwrap();
                        *offset += cmd_len;
                        println!("Replication offset: {}", offset);
                    }
                    _ => panic!("Unknown replicaiton cmd: {}", data),
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    pub fn handle_connection(&self, stream: TcpStream) -> Result<()> {
        println!("Start handing queries...");

        let mut conn = Connection::new(stream);
        loop {
            let res = conn.read_data();

            match res {
                Ok(data) => self.handle_data(&mut conn, data)?,
                Err(error) => {
                    println!("Error: {}, will close connection", error);
                    break;
                }
            }
        }

        Ok(())
    }

    fn handle_data(&self, conn: &mut Connection, data: Data) -> Result<()> {
        println!("Recv: {}", data);
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
                        let store = self.store.lock().unwrap();

                        assert_eq!(vs.len(), 2);
                        let key = string_at(1)?;
                        match store.get(&key) {
                            None => conn.write_data(Data::NullBulkString)?,
                            Some(value) => {
                                conn.write_data(Data::BulkString(value.to_string().into()))?
                            }
                        }
                    }
                    "set" => {
                        let store = self.store.lock().unwrap();

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

                        store.set(key, Value::String(value), expire_in);
                        conn.write_data(Data::SimpleString("OK".into()))?
                    }
                    "info" => match string_at(1)?.to_ascii_lowercase().as_str() {
                        "replication" => {
                            let role = String::from("role:slave");
                            let replication_id =
                                format!("master_replid:{}", self.master_replication_id);
                            let replication_offset = format!(
                                "master_repl_offset:{}",
                                self.replication_offset.lock().unwrap()
                            );

                            conn.write_data(Data::BulkString(
                                vec![role, replication_id, replication_offset]
                                    .join("\n")
                                    .into(),
                            ))?
                        }
                        info_type => panic!("unknown info type: {}", info_type),
                    },
                    command => println!("unknown command: {}", command),
                }
            }
            _ => panic!("Unknown: {}", data),
        };

        Ok(())
    }
}
