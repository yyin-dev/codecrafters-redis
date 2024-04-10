use std::net::SocketAddr;

#[derive(Clone, Debug)]
pub enum Mode {
    Master,
    Slave(SocketAddr),
}
