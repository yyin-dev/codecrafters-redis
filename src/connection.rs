use crate::data::DecodeError;
use crate::data::{decode_rdb_file, Data};
use anyhow::{anyhow, Result};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::{io::Read, net::TcpStream};

struct Buffer {
    stream: TcpStream,
    buffer: Vec<u8>,
}

impl Buffer {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buffer: Vec::new(),
        }
    }

    fn load_more(&mut self) -> Result<()> {
        let mut buf = vec![0; 1024];
        let num_bytes_read = self.stream.read(&mut buf)?;
        if num_bytes_read == 0 {
            // TcpStream::read returning 0 means the connection is closed
            Err(anyhow!("TcpStream closed"))
        } else {
            self.buffer.append(&mut buf[..num_bytes_read].to_vec());
            Ok(())
        }
    }

    pub fn read_data(&mut self) -> Result<Data> {
        // Try serving the data from the buffer;
        // If not, read more bytes from the stream;
        // Always remember to adjust the buffer properly for consumed bytes

        match Data::decode(&self.buffer) {
            Ok((data, num_bytes)) => {
                self.buffer = self.buffer[num_bytes..].to_vec();
                Ok(data)
            }
            Err(err) => {
                if let Some(DecodeError::NeedMoreBytes) = err.downcast_ref::<DecodeError>() {
                    self.load_more()?;
                    self.read_data()
                } else {
                    Err(err)
                }
            }
        }
    }

    pub fn read_rdb_file(&mut self) -> Result<Vec<u8>> {
        // Basically the same as read_data
        match decode_rdb_file(&self.buffer) {
            Ok((data, num_bytes)) => {
                self.buffer = self.buffer[num_bytes..].to_vec();
                Ok(data)
            }
            Err(err) => {
                if let Some(DecodeError::NeedMoreBytes) = err.downcast_ref::<DecodeError>() {
                    self.load_more()?;
                    self.read_rdb_file()
                } else {
                    Err(err)
                }
            }
        }
    }

    pub fn write_data(&mut self, data: Data) -> Result<()> {
        Ok(self.stream.write_all(&data.encode())?)
    }

    pub fn write(&mut self, buf: Vec<u8>) -> Result<()> {
        Ok(self.stream.write_all(&buf)?)
    }
}

/// `Connection` is thread-safe
pub struct Connection {
    buffer: Arc<Mutex<Buffer>>,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        let buffer = Arc::new(Mutex::new(Buffer::new(stream)));
        Self { buffer }
    }

    pub fn read_data(&self) -> Result<Data> {
        self.buffer.lock().unwrap().read_data()
    }

    pub fn read_rdb_file(&self) -> Result<Vec<u8>> {
        self.buffer.lock().unwrap().read_rdb_file()
    }

    pub fn write_data(&self, data: Data) -> Result<()> {
        self.buffer.lock().unwrap().write_data(data)
    }

    pub fn write(&self, buf: Vec<u8>) -> Result<()> {
        self.buffer.lock().unwrap().write(buf)
    }
}
