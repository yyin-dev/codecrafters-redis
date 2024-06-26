use crate::connection::Connection;
use crate::data::{self, Data};
use crate::mode::MasterParams;
use crate::rdb::Rdb;
use crate::store::Store;
use crate::stream::{Entry, EntryId};
use crate::value::Value;
use anyhow::anyhow;
use anyhow::Result;
use base64::Engine;
use crossbeam_channel::select;
use std::collections::HashMap;
use std::ops::Bound::{Excluded, Included};
use std::path::PathBuf;
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
    dir: Option<PathBuf>,
    dbfilename: Option<String>,
    rdb: Rdb,
    inner: Arc<Mutex<MasterInner>>,
}

fn entries_to_array(entries: Vec<(EntryId, Vec<Entry>)>) -> Data {
    let data = entries
        .into_iter()
        .map(|(entryid, entries)| {
            Data::Array(vec![
                Data::BulkString(entryid.to_string().into()),
                Data::Array(
                    entries
                        .into_iter()
                        .flat_map(|entry| {
                            vec![
                                Data::BulkString(entry.key.into()),
                                Data::BulkString(entry.value.into()),
                            ]
                        })
                        .collect(),
                ),
            ])
        })
        .collect();

    Data::Array(data)
}

impl Master {
    pub fn new(params: MasterParams) -> Result<Self> {
        let path = match (params.dir.clone(), params.dbfilename.clone()) {
            (None, _) | (_, None) => None,
            (Some(mut dir), Some(dbfilename)) => {
                dir.push(dbfilename);
                Some(dir)
            }
        };
        let rdb = Rdb::read(path)?;
        println!("Rdb: {:?}", rdb.store.data());

        let store = Store::new();
        for (k, v) in rdb.store.data().iter() {
            store.set(k.clone(), v.clone(), None);
        }

        let inner = MasterInner {
            replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".into(),
            replication_offset: 0,
            store,
            replicas: Vec::new(),
        };

        let master = Self {
            dir: params.dir,
            dbfilename: params.dbfilename,
            rdb,
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
                    "keys" => {
                        assert_eq!(vs.len(), 2);
                        assert_eq!(string_at(1)?, "*");

                        let keys = self
                            .rdb
                            .store
                            .data()
                            .keys()
                            .map(|k| Data::BulkString(k.as_str().into()))
                            .collect();
                        conn.write_data(Data::Array(keys))?
                    }

                    "get" => {
                        let inner = self.inner.lock().unwrap();

                        assert_eq!(vs.len(), 2);
                        let key = string_at(1)?;
                        match inner.store.get(&key) {
                            None => conn.write_data(Data::NullBulkString)?,
                            Some(value) => {
                                conn.write_data(Data::BulkString(value.to_string().into()))?
                            }
                        }
                    }
                    "type" => {
                        let inner = self.inner.lock().unwrap();

                        assert_eq!(vs.len(), 2);
                        let key = string_at(1)?;
                        let t = inner.store.get_type(key);
                        conn.write_data(Data::SimpleString(t.into()))?
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

                        inner.store.set(key, Value::String(value), expire_in);
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
                    "xadd" => {
                        // xadd <stream> <entry-id> <e1 key> <e1 value>
                        assert!(vs.len() >= 5);
                        assert!(vs.len() % 2 == 1);

                        let stream = string_at(1)?;
                        let entry_id = string_at(2)?;

                        let kvs = vs[3..]
                            .chunks_exact(2)
                            .map(|data| {
                                let k = data[0].get_string().unwrap();
                                let v = data[1].get_string().unwrap();
                                (k, v)
                            })
                            .collect();

                        let res = self.inner.lock().unwrap().store.stream_set(
                            stream.clone(),
                            entry_id.clone(),
                            kvs,
                        );

                        match res {
                            Ok(entry_id) => {
                                conn.write_data(Data::BulkString(entry_id.to_string().into()))?
                            }
                            Err(err) => {
                                conn.write_data(Data::SimpleError(err.to_string()))?;
                                return Ok(false);
                            }
                        }
                    }
                    "xrange" => {
                        // xrange <stream> <start> <end>
                        assert_eq!(vs.len(), 4);

                        let stream = string_at(1)?;

                        let entries = self.inner.lock().unwrap().store.get_stream_range(
                            stream,
                            Included(EntryId::create_start(string_at(2)?)?),
                            Included(EntryId::create_end(string_at(3)?)?),
                        )?;

                        conn.write_data(entries_to_array(entries))?
                    }
                    "xread" => {
                        // xread [blocks <timeout>] streams <stream1> <entryid1> <stream2> <entryid2>
                        assert_eq!(vs.len() % 2, 0);

                        let (timeout, stream_start_idx) = if string_at(1)? == "block" {
                            let mill = match string_at(2)?.parse::<u64>()? {
                                0 => u64::MAX,
                                mill => mill,
                            };

                            (Some(Duration::from_millis(mill)), 4)
                        } else {
                            (None, 2)
                        };
                        let num_streams = (vs.len() - stream_start_idx) / 2;

                        let mut streams_and_start = Vec::new();
                        for i in stream_start_idx..stream_start_idx + num_streams {
                            let stream = string_at(i)?;
                            let start = string_at(i + num_streams)?;
                            streams_and_start.push((stream, start));
                        }

                        let mut curr_max_entry_ids = HashMap::new();
                        {
                            let inner = self.inner.lock().unwrap();
                            streams_and_start.iter().for_each(|(stream, _)| {
                                let curr_max = inner.store.get_stream_curr_max_id(stream.clone());
                                curr_max_entry_ids.insert(stream.clone(), curr_max);
                            });
                        }

                        let get_stream_and_entries = |convert_wildcard: bool| {
                            let inner = self.inner.lock().unwrap();
                            streams_and_start
                                .iter()
                                .filter_map(|(stream, start)| {
                                    let start = if start == "$" {
                                        if convert_wildcard {
                                            curr_max_entry_ids.get(stream).unwrap().clone()
                                        } else {
                                            return None;
                                        }
                                    } else {
                                        EntryId::create_start(start.clone()).unwrap()
                                    };

                                    let entries = inner
                                        .store
                                        .get_stream_range(
                                            stream.clone(),
                                            Excluded(start),
                                            Included(EntryId::max()),
                                        )
                                        .unwrap();

                                    if entries.is_empty() {
                                        None
                                    } else {
                                        Some((stream.clone(), entries))
                                    }
                                })
                                .collect::<Vec<_>>()
                        };

                        let mut stream_and_entries = get_stream_and_entries(false);
                        println!("Streams and entries: {:?}", stream_and_entries);

                        if stream_and_entries.is_empty() && timeout.is_some() {
                            // Blocks waiting

                            // TODO: Handle more than one
                            let (stream, entry_id) = streams_and_start[0].clone();
                            let update_chan = {
                                let mut inner = self.inner.lock().unwrap();
                                let entry_id = if entry_id == "$" {
                                    inner.store.get_stream_curr_max_id(stream.clone())
                                } else {
                                    EntryId::create_start(entry_id.clone()).unwrap()
                                };
                                inner
                                    .store
                                    .stream_subscribe(stream.clone(), entry_id.clone())
                            };

                            println!("Blocking for updates for {}, {}", stream, entry_id);
                            select! {
                                recv(update_chan) -> msg => match msg {
                                    Err(err) =>  println!("Error receiving update: {}", err),
                                    Ok(()) => {
                                        println!("Received update, will query again...");
                                        stream_and_entries = get_stream_and_entries(true);
                                    }
                                },
                                default(timeout.unwrap()) => println!("Timeout!"),
                            }
                        }

                        if stream_and_entries.is_empty() {
                            conn.write_data(Data::NullBulkString)?
                        } else {
                            let as_arrays = stream_and_entries
                                .into_iter()
                                .map(|(stream, entries)| {
                                    let stream = Data::BulkString(stream.into());
                                    let entries = entries_to_array(entries);
                                    Data::Array(vec![stream, entries])
                                })
                                .collect::<Vec<_>>();
                            conn.write_data(Data::Array(as_arrays))?
                        }
                    }
                    "config" => {
                        assert_eq!(vs.len(), 3);
                        assert_eq!(vs[1].get_string().unwrap().to_ascii_lowercase(), "get");
                        match string_at(2)?.to_ascii_lowercase().as_str() {
                            "dir" => {
                                let dir = self
                                    .dir
                                    .as_ref()
                                    .map(|p| p.clone().into_os_string().into_string())
                                    .unwrap()
                                    .unwrap();
                                conn.write_data(Data::Array(vec![
                                    Data::BulkString("dir".into()),
                                    Data::BulkString(dir.into()),
                                ]))?
                            }
                            "dbfilename" => {
                                let dbfilename = self.dbfilename.as_ref().unwrap().to_string();
                                conn.write_data(Data::Array(vec![
                                    Data::BulkString("dbfilename".into()),
                                    Data::BulkString(dbfilename.into()),
                                ]))?
                            }
                            _ => unreachable!(),
                        };
                    }
                    "info" => match string_at(1)?.to_ascii_lowercase().as_str() {
                        "replication" => {
                            let inner = self.inner.lock().unwrap();
                            let role = String::from("role:master");
                            let replication_id = format!("master_replid:{}", inner.replication_id);
                            let replication_offset =
                                format!("master_repl_offset:{}", inner.replication_offset);

                            conn.write_data(Data::BulkString(
                                [role, replication_id, replication_offset].join("\n").into(),
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
                        let num_replicas_to_wait = string_at(1)?.parse::<usize>()?;
                        let timeout = Duration::from_millis(string_at(2)?.parse()?);
                        self.handle_wait(conn, num_replicas_to_wait, timeout)?
                    }
                    command => panic!("unknown command: {}", command),
                }
            }
            v => println!("Unkonwn: {:?}", v),
        };

        Ok(false)
    }

    fn handle_wait(
        &self,
        conn: &mut Connection,
        num_replicas_to_wait: usize,
        timeout: Duration,
    ) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();

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
                // Implement timeout: https://stackoverflow.com/a/42720480/9057530
                let (tx, rx) = mpsc::channel();
                let replication_offset = inner.replication_offset;
                let cnt = Arc::new(Mutex::new(0));

                let replicas = inner.replicas.clone();

                {
                    let cnt = cnt.clone();

                    // The idea is to query replicas for replicated offsets.
                    //
                    // Two possible ways to implement this:
                    // 1. Query all replicas in order, in one thread.
                    // 2. Spawn one thread for each replica and query offsets in parallel.
                    //
                    // The 1st approach is simpler and passes the tests. The 2nd approach
                    // is more correct but doesn't pass the tests.
                    //
                    // The following events happen in the test:
                    //
                    // Start 3 replicas and 1 master
                    // to master: Set foo 123 (which gets replicated to all 3 replicas)
                    // to master: WAIT 1 500
                    // Only replica-1 responds REPLCONF ACK
                    //
                    // to master: SET bar 456 (which gets replicated to all 3 replicas)
                    // to master: WAIT 3 500
                    // Only replica-1 and replica-2 reponds REPLCONF ACK
                    //
                    // If we implement the 2nd approach, when the master is querying replica-2
                    // for offset after "SET bar", a thread is still blocked waiting
                    // for REPLCONF ACK from replica-2 for "SET foo". In other words,
                    // two threads are waiting for REPLCONF ACK from replica-2, but
                    // only one is sent.
                    // This is not a problem for the 1st approach because we wouldn't
                    // try to query replica-2's offset.
                    std::thread::spawn(move || -> Result<()> {
                        for r in replicas.iter() {
                            let r = r.clone();
                            println!("Waiting replica {} response", r.id);
                            let data = r.conn.read_data()?;
                            if let Data::Array(vs) = data {
                                let string_at = |idx: usize| -> Result<String> {
                                    vs[idx].get_string().ok_or(anyhow!("fail to get string"))
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
                                        if offset >= replication_offset {
                                            let mut cnt = cnt.lock().unwrap();
                                            *cnt += 1;

                                            if *cnt == num_replicas_to_wait {
                                                tx.send(()).unwrap();
                                                break;
                                            }
                                        };
                                    }
                                    _ => unreachable!(),
                                }
                            } else {
                                unreachable!()
                            }
                        }
                        Ok(())
                    });
                }

                if let Err(err) = rx.recv_timeout(timeout) {
                    println!("Timeout: {}", err);
                };

                let cnt = *cnt.lock().unwrap();
                cnt
            };
            println!("cnt: {}", cnt);

            inner.replication_offset += getack.num_bytes();
            println!("replication offset: +{}", getack.num_bytes());
            conn.write_data(Data::Integer(cnt as i64))
        } else {
            conn.write_data(Data::Integer(inner.replicas.len() as i64))
        }
    }
}
