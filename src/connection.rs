use crate::data::Data;
use crate::data::DecodeError;
use anyhow::Result;
use std::{io::Read, net::TcpStream};

pub struct Connection {
    stream: TcpStream,
    buffer: Vec<u8>,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buffer: Vec::new(),
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
                    let mut buf = vec![0; 1024];
                    let num_bytes_read = self.stream.read(&mut buf)?;
                    self.buffer.append(&mut buf[..num_bytes_read].to_vec());
                    self.read_data()
                } else {
                    Err(err)
                }
            }
        }
    }
}
