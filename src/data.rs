#![allow(unused_variables)]
#![allow(dead_code)]
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

fn decode_bulk_string(buf: &[u8]) -> Result<(Data, usize)> {
    assert_eq!(buf[0] as char, '$');

    // Parse length, handling null bulk string
    if buf[1] as char == '-' {
        // null bulk string
        assert_eq!(&buf[..5], NULL_BULK_STRING.as_bytes());
        Ok((Data::NullBulkString, 5))
    } else {
        let mut curr = 1;

        let length: usize = {
            let mut length_buf = String::new();
            while curr < buf.len() {
                let c = buf[curr] as char;
                if char::is_numeric(c) {
                    length_buf.push(c);
                } else {
                    break;
                }

                curr += 1;
            }
            length_buf.parse().unwrap()
        };

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
        roundtrip(Data::SimpleString(String::from("").into()));
        roundtrip(Data::SimpleString(String::from("abc").into()));
    }

    #[test]
    fn bulk_string() {
        roundtrip(Data::BulkString(String::from("").into()));
        roundtrip(Data::BulkString(String::from("abc").into()));
        roundtrip(Data::NullBulkString);
    }
}
