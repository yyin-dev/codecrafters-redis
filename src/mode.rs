use std::{net::SocketAddr, path::PathBuf};

#[derive(Clone, Debug)]
pub struct MasterParams {
    pub dir: Option<PathBuf>,
    pub dbfilename: Option<String>,
}

#[derive(Clone, Debug)]
pub struct SlaveParams {
    pub master_sockaddr : SocketAddr,
}

#[derive(Clone, Debug)]
pub enum Mode {
    Master(MasterParams),
    Slave(SlaveParams),
}
