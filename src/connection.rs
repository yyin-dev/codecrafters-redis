use crate::data::DecodeError;
use crate::data::{decode_rdb_file, Data};
use anyhow::{anyhow, Result};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::{io::Read, net::TcpStream};

pub struct Connection {
    buffer: Arc<Mutex<Vec<u8>>>,
    stream: Arc<TcpStream>,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        Self {
            buffer,
            stream: Arc::new(stream),
        }
    }

    fn load_more(&self) -> Result<()> {
        let mut buf = vec![0; 1024];
        let num_bytes_read = self.stream.as_ref().read(&mut buf)?;
        if num_bytes_read == 0 {
            // TcpStream::read returning 0 means the connection is closed
            Err(anyhow!("TcpStream closed"))
        } else {
            self.buffer
                .lock()
                .unwrap()
                .append(&mut buf[..num_bytes_read].to_vec());
            Ok(())
        }
    }

    pub fn read_data(&self) -> Result<Data> {
        // Try serving the data from the buffer;
        // If not, read more bytes from the stream;
        // Always remember to adjust the buffer properly for consumed bytes
        let mut buffer = self.buffer.lock().unwrap();

        match Data::decode(&buffer) {
            Ok((data, num_bytes)) => {
                *buffer = buffer[num_bytes..].to_vec();
                Ok(data)
            }
            Err(err) => {
                if let Some(DecodeError::NeedMoreBytes) = err.downcast_ref::<DecodeError>() {
                    // Release lock!
                    drop(buffer);

                    self.load_more()?;
                    self.read_data()
                } else {
                    Err(err)
                }
            }
        }
    }

    pub fn read_rdb_file(&self) -> Result<Vec<u8>> {
        // Basically the same as read_data
        let mut buffer = self.buffer.lock().unwrap();
        match decode_rdb_file(&buffer) {
            Ok((data, num_bytes)) => {
                *buffer = buffer[num_bytes..].to_vec();
                Ok(data)
            }
            Err(err) => {
                if let Some(DecodeError::NeedMoreBytes) = err.downcast_ref::<DecodeError>() {
                    // Release lock!
                    drop(buffer);

                    self.load_more()?;
                    self.read_rdb_file()
                } else {
                    Err(err)
                }
            }
        }
    }

    /// `write_data` is not thread-safe
    pub fn write_data(&self, data: Data) -> Result<()> {
        Ok(self.stream.as_ref().write_all(&data.encode())?)
    }

    /// `write` is not thread-safe
    pub fn write(&self, buf: Vec<u8>) -> Result<()> {
        Ok(self.stream.as_ref().write_all(&buf)?)
    }
}
