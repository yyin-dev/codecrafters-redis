use anyhow::Result;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Data {
    SimpleString(Vec<u8>),
    BulkString(Vec<u8>),
    NullBulkString,
    Array(Vec<Data>),
    Unknown(Vec<u8>),
}

const NULL_BULK_STRING: &str = "$-1\r\n";

fn append_crlf(s: &mut Vec<u8>) {
    s.append(&mut vec!['\r' as u8, '\n' as u8])
}

fn encode_simple_string(mut s: Vec<u8>) -> Vec<u8> {
    // +<data>\r\n
    s.insert(0, '+' as u8);
    append_crlf(&mut s);
    s
}

fn encode_bulk_string(mut s: Vec<u8>) -> Vec<u8> {
    // $<length>\r\n<data>\r\n
    let mut res = Vec::new();
    res.append(&mut vec!['$' as u8]);
    res.append(&mut s.len().to_string().as_bytes().to_vec());
    append_crlf(&mut res);
    res.append(&mut s);
    append_crlf(&mut res);
    res
}

fn encode_null_bulk_string() -> Vec<u8> {
    NULL_BULK_STRING.into()
}

fn encode_array(vs: Vec<Data>) -> Vec<u8> {
    // *<number-of-elements>\r\n<element-1>...<element-n>
    let mut res = Vec::new();
    res.append(&mut vec!['*' as u8]);
    res.append(&mut vs.len().to_string().as_bytes().to_vec());
    append_crlf(&mut res);
    for v in vs {
        res.append(&mut v.encode());
    }
    res
}

fn decode_number(buf: &[u8]) -> Option<(usize, usize)> {
    let mut num_str = String::new();

    for byte in buf {
        if char::is_numeric(*byte as char) {
            num_str.push(*byte as char);
        } else {
            break;
        }
    }

    match num_str.is_empty() {
        true => None,
        false => {
            let num_bytes = num_str.len();
            num_str.parse::<usize>().ok().map(|v| (v, num_bytes))
        }
    }
}

fn decode_bulk_string(buf: &[u8]) -> Result<(Data, usize)> {
    assert_eq!(buf[0] as char, '$');

    // Parse length, handling null bulk string
    if buf[1] as char == '-' {
        // null bulk string
        assert_eq!(&buf[..5], NULL_BULK_STRING.as_bytes());
        Ok((Data::NullBulkString, 5))
    } else {
        let mut curr = 1;

        let (length, num_bytes_consumed) = decode_number(&buf[curr..]).unwrap();
        curr += num_bytes_consumed;

        // Check \r\n
        assert_eq!(buf[curr] as char, '\r');
        curr += 1;
        assert_eq!(buf[curr] as char, '\n');
        curr += 1;

        // Extract data
        let s = &buf[curr..curr + length];

        // Check \r\n
        curr += length;
        assert_eq!(buf[curr] as char, '\r');
        curr += 1;
        assert_eq!(buf[curr] as char, '\n');
        curr += 1;

        Ok((Data::BulkString(s.into()), curr))
    }
}

fn decode_simple_string(buf: &[u8]) -> Result<(Data, usize)> {
    assert_eq!(buf[0] as char, '+');

    let mut end = 1;
    while end < buf.len() && !char::is_whitespace(buf[end] as char) {
        end += 1;
    }

    assert_eq!(buf[end] as char, '\r');
    assert_eq!(buf[end + 1] as char, '\n');

    Ok((Data::SimpleString(buf[1..end].into()), end + 2))
}

fn decode_array(buf: &[u8]) -> Result<(Data, usize)> {
    assert_eq!(buf[0] as char, '*');

    let mut curr = 1;

    let (length, num_bytes) = decode_number(&buf[curr..]).unwrap();
    curr += num_bytes;

    // \r\n
    assert_eq!(buf[curr] as char, '\r');
    curr += 1;
    assert_eq!(buf[curr] as char, '\n');
    curr += 1;

    let mut values = Vec::new();
    for _ in 0..length {
        let (data, num_bytes) = Data::decode(&buf[curr..])?;
        values.push(data);
        curr += num_bytes;
    }

    Ok((Data::Array(values), curr))
}

impl Data {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Data::SimpleString(s) => encode_simple_string(s.clone()),
            Data::BulkString(s) => encode_bulk_string(s.clone()),
            Data::NullBulkString => encode_null_bulk_string(),
            Data::Array(arr) => encode_array(arr.to_vec()),
            Data::Unknown(_) => panic!("encode Unknown?"),
        }
    }

    pub fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        match buf[0] as char {
            '+' => decode_simple_string(buf),
            '$' => decode_bulk_string(buf),
            '*' => decode_array(buf),
            c => Err(anyhow::anyhow!("Unrecognized data type: {}", c)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(data: Data) {
        let encoded = data.encode();
        let (decoded, num_bytes) = Data::decode(&encoded).unwrap();
        assert_eq!(num_bytes, encoded.len());
        assert_eq!(data, decoded);
    }

    #[test]
    fn simple_string() {
        roundtrip(Data::SimpleString("".into()));
        roundtrip(Data::SimpleString("abc".into()));
    }

    #[test]
    fn bulk_string() {
        roundtrip(Data::BulkString("".into()));
        roundtrip(Data::BulkString("abc".into()));
        roundtrip(Data::NullBulkString);
    }

    #[test]
    fn array() {
        roundtrip(Data::Array(Vec::new()));
        roundtrip(Data::Array(vec![Data::SimpleString("a".into())]));
        roundtrip(Data::Array(vec![
            Data::SimpleString("a".into()),
            Data::SimpleString("ab".into()),
        ]));
        roundtrip(Data::Array(vec![
            Data::SimpleString("a".into()),
            Data::SimpleString("ab".into()),
            Data::BulkString("abc".into()),
        ]));
        roundtrip(Data::Array(vec![
            Data::SimpleString("a".into()),
            Data::SimpleString("ab".into()),
            Data::BulkString("abc".into()),
            Data::Array(vec![Data::SimpleString("abcd".into())]),
        ]));
    }
}
