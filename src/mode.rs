use std::net::SocketAddr;

#[derive(Debug)]
pub enum Mode {
    Master,
    Slave(SocketAddr),
}
