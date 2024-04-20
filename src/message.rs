use redis_protocol::resp2::types::OwnedFrame;

pub fn ok() -> OwnedFrame {
    OwnedFrame::SimpleString("OK".into())
}

pub fn pong() -> OwnedFrame {
    OwnedFrame::SimpleString("PONG".into())
}
