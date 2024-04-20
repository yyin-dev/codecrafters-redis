use anyhow::bail;
use anyhow::Result;
use thiserror::Error;

const NULL_BULK_STRING: &str = "$-1\r\n";
const SIMPLE_STRING_DATA_TYPE: char = '+';
const BULK_STRING_DATA_TYPE: char = '$';
const ARRAY_DATA_TYPE: char = '*';

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Data {
    SimpleString(Vec<u8>),
    BulkString(Vec<u8>),
    NullBulkString,
    Array(Vec<Data>),
    Unknown(Vec<u8>),
}

fn append_crlf(s: &mut Vec<u8>) {
    s.append(&mut vec!['\r' as u8, '\n' as u8])
}

fn encode_simple_string(mut s: Vec<u8>) -> Vec<u8> {
    // +<data>\r\n
    s.insert(0, SIMPLE_STRING_DATA_TYPE as u8);
    append_crlf(&mut s);
    s
}

fn encode_bulk_string(mut s: Vec<u8>) -> Vec<u8> {
    // $<length>\r\n<data>\r\n
    let mut res = Vec::new();
    res.append(&mut vec![BULK_STRING_DATA_TYPE as u8]);
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
    res.append(&mut vec![ARRAY_DATA_TYPE as u8]);
    res.append(&mut vs.len().to_string().as_bytes().to_vec());
    append_crlf(&mut res);
    for v in vs {
        res.append(&mut v.encode());
    }
    res
}

pub fn encode_rdb_file(rdb: Vec<u8>) -> Vec<u8> {
    let as_bulk_string = encode_bulk_string(rdb);
    let len = as_bulk_string.len();
    as_bulk_string[..(len - 2)].to_vec()
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("need more bytes")]
    NeedMoreBytes,
    #[error("cannot decode number")]
    CannotDecodeNumber,
}

fn decode_number(buf: &[u8]) -> Result<(usize, usize)> {
    let mut num_str = String::new();

    for byte in buf {
        if char::is_numeric(*byte as char) {
            num_str.push(*byte as char);
        } else {
            break;
        }
    }

    // Expect more non-numeric bytes, at least for bulk string and arrays
    if num_str.len() == buf.len() {
        bail!(DecodeError::NeedMoreBytes)
    };

    match num_str.is_empty() {
        true => bail!(DecodeError::CannotDecodeNumber),
        false => {
            let num_bytes = num_str.len();
            Ok(num_str.parse::<usize>().map(|v| (v, num_bytes))?)
        }
    }
}

fn decode_bulk_string(buf: &[u8]) -> Result<(Data, usize)> {
    // Shortest bulk string: $0\r\n. 4 bytes
    if buf.len() < 4 {
        bail!(DecodeError::NeedMoreBytes)
    }

    assert_eq!(buf[0] as char, BULK_STRING_DATA_TYPE);

    // Parse length, handling null bulk string
    if buf[1] as char == '-' {
        if buf.len() < 5 {
            bail!(DecodeError::NeedMoreBytes)
        }

        // null bulk string
        assert_eq!(&buf[..5], NULL_BULK_STRING.as_bytes());
        Ok((Data::NullBulkString, 5))
    } else {
        let mut curr = 1;

        let (length, num_bytes_consumed) = decode_number(&buf[curr..])?;
        curr += num_bytes_consumed;

        // Check \r\n
        if buf.len() < curr + 2 {
            bail!(DecodeError::NeedMoreBytes)
        }
        assert_eq!(buf[curr] as char, '\r');
        curr += 1;
        assert_eq!(buf[curr] as char, '\n');
        curr += 1;

        // Extract data
        if buf.len() < curr + length {
            bail!(DecodeError::NeedMoreBytes)
        }
        let s = &buf[curr..curr + length];
        curr += length;

        // Check \r\n
        if buf.len() < curr + 2 {
            bail!(DecodeError::NeedMoreBytes)
        }
        assert_eq!(buf[curr] as char, '\r');
        curr += 1;
        assert_eq!(buf[curr] as char, '\n');
        curr += 1;

        Ok((Data::BulkString(s.into()), curr))
    }
}

fn decode_simple_string(buf: &[u8]) -> Result<(Data, usize)> {
    // Shortest simple string: +\r\n. 3 bytes
    if buf.len() < 3 {
        bail!(DecodeError::NeedMoreBytes)
    }

    assert_eq!(buf[0] as char, SIMPLE_STRING_DATA_TYPE);

    let mut curr = 1;
    while curr < buf.len() && (buf[curr] as char != '\r') {
        curr += 1;
    }

    //\r\n
    if buf.len() < curr + 2 {
        bail!(DecodeError::NeedMoreBytes)
    }
    assert_eq!(buf[curr] as char, '\r');
    assert_eq!(buf[curr + 1] as char, '\n');

    Ok((Data::SimpleString(buf[1..curr].into()), curr + 2))
}

fn decode_array(buf: &[u8]) -> Result<(Data, usize)> {
    // Shortest array: *0\r\n. 4 bytes
    if buf.len() < 4 {
        bail!(DecodeError::NeedMoreBytes)
    }

    assert_eq!(buf[0] as char, ARRAY_DATA_TYPE);

    let mut curr = 1;

    let (length, num_bytes) = decode_number(&buf[curr..]).unwrap();
    curr += num_bytes;

    // \r\n
    if buf.len() < curr + 2 {
        bail!(DecodeError::NeedMoreBytes)
    }
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

pub fn decode_rdb_file(buf: &[u8]) -> Result<(Vec<u8>, usize)> {
    if buf.len() < 4 {
        bail!(DecodeError::NeedMoreBytes)
    }

    // Format: $<length_of_file>\r\n<contents_of_file>
    assert_eq!(buf[0] as char, '$');

    // length
    let mut curr = 1;
    let (length, num_bytes) = decode_number(&buf[curr..])?;
    curr += num_bytes;

    // \r\n
    if buf.len() < curr + 2 {
        bail!(DecodeError::NeedMoreBytes)
    }
    assert_eq!(buf[curr] as char, '\r');
    curr += 1;
    assert_eq!(buf[curr] as char, '\n');
    curr += 1;

    // data
    if buf.len() < curr + length {
        bail!(DecodeError::NeedMoreBytes)
    }

    Ok((buf[curr..curr + length].into(), curr + length))
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
        if buf.len() == 0 {
            bail!(DecodeError::NeedMoreBytes)
        }

        match buf[0] as char {
            SIMPLE_STRING_DATA_TYPE => decode_simple_string(buf),
            BULK_STRING_DATA_TYPE => decode_bulk_string(buf),
            ARRAY_DATA_TYPE => decode_array(buf),
            c => Err(anyhow::anyhow!("Unrecognized data type: {}", c)),
        }
    }

    pub fn num_bytes(&self) -> usize {
        match self {
            Data::SimpleString(s) => 1 + s.len() + 2,
            Data::BulkString(s) => 1 + s.len().to_string().len() + 2 + s.len() + 2,
            Data::NullBulkString => 5,
            Data::Array(vs) => {
                1 + vs.len().to_string().len() + 2 + vs.iter().map(|v| v.num_bytes()).sum::<usize>()
            }
            Data::Unknown(_) => usize::MAX,
        }
    }

    pub fn to_string(&self) -> Option<String> {
        match self {
            Data::SimpleString(s) | Data::BulkString(s) => String::from_utf8(s.to_vec()).ok(),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(data: Data) {
        let encoded = data.encode();
        assert_eq!(data.num_bytes(), encoded.len());
        let (decoded, num_bytes) = Data::decode(&encoded).unwrap();
        assert_eq!(num_bytes, encoded.len());
        assert_eq!(data, decoded);
    }

    #[test]
    fn simple_string() {
        roundtrip(Data::SimpleString("".into()));
        roundtrip(Data::SimpleString("abc".into()));
        roundtrip(Data::SimpleString("abc d".into()));
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

    #[test]
    fn rdb_file() {
        assert!(decode_rdb_file("$2\r\nx".as_bytes()).is_err());
        assert!(decode_rdb_file("$2\r\nxy".as_bytes()).is_ok());
    }

    #[test]
    fn decode_simple_string_error() {
        assert!(Data::decode("+\r".as_bytes()).is_err());
        assert!(Data::decode("+\n".as_bytes()).is_err());
    }

    #[test]
    fn decode_bulk_string_error() {
        assert!(Data::decode("$".as_bytes()).is_err());
        assert!(Data::decode("$2".as_bytes()).is_err());
        assert!(Data::decode("$2\r".as_bytes()).is_err());
        assert!(Data::decode("$2\r\n".as_bytes()).is_err());
        assert!(Data::decode("$2\r\n\r".as_bytes()).is_err());
        assert!(Data::decode("$2\r\n\r\n".as_bytes()).is_err());
        assert!(Data::decode("$2\r\na\r\n".as_bytes()).is_err());
    }

    #[test]
    fn decode_array_error() {
        assert!(Data::decode("*0".as_bytes()).is_err());
        assert!(Data::decode("*0\r".as_bytes()).is_err());
        assert!(Data::decode("*1\r\n".as_bytes()).is_err());
        assert!(Data::decode("*1\r\n$".as_bytes()).is_err());
        assert!(Data::decode("*1\r\n+".as_bytes()).is_err());
        assert!(Data::decode("*1\r\n+OK".as_bytes()).is_err());
        assert!(Data::decode("*1\r\n+OK\r".as_bytes()).is_err());
        assert!(Data::decode("*2\r\n+OK\r\n".as_bytes()).is_err());
    }
}
