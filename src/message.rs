use redis_protocol::resp2::types::OwnedFrame;

fn to_bulk_string_array(strs: Vec<String>) -> OwnedFrame {
    OwnedFrame::Array(
        strs.into_iter()
            .map(|s| OwnedFrame::BulkString(s.into()))
            .collect(),
    )
}

pub fn ok() -> OwnedFrame {
    OwnedFrame::SimpleString("OK".into())
}

pub fn pong() -> OwnedFrame {
    OwnedFrame::SimpleString("PONG".into())
}

pub fn psync() -> OwnedFrame {
    to_bulk_string_array(vec!["PSYNC".into(), "?".into(), "-1".into()])
}
