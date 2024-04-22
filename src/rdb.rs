use crate::value::Value;
use anyhow::Result;
use std::{
    fs::File,
    io::{BufReader, Read},
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::store::Store;

pub struct Rdb {
    pub store: Store,
}

const EOF: u8 = 0xff;
const SELECTDB: u8 = 0xfe;
const EXP_MS: u8 = 0xfc;
const RESIZEDB: u8 = 0xfb;
const AUX: u8 = 0xfa;

mod value_code {
    pub const STRING: u8 = 0;
}

fn decode_length_00(first_byte: u8) -> Result<usize> {
    Ok(first_byte as usize)
}

fn decode_length_01(bytes: [u8; 2]) -> Result<usize> {
    // 01aa bbbb, cccc dddd
    // little-endian: most significant bit at higher address (the 2nd byte)
    // The number looks like: 00cc ccdd, ddaa bbbb
    // ddaa bbbb, 00cc ccdd

    // 0000 0000 00aa bbbb
    let first_byte = (bytes[0] & 0b0011_1111) as u16;
    // 0000 0000 cccc dddd
    let second_byte = bytes[1] as u16;

    Ok(((second_byte << 6) | first_byte) as usize)
}

enum Length {
    EncodedAsInt(usize),
    EncodedAsString(usize),
}

impl Length {
    pub fn to_usize(&self) -> usize {
        match self {
            Self::EncodedAsInt(v) | Self::EncodedAsString(v) => *v,
        }
    }
}

fn decode_length<R: Read>(reader: &mut BufReader<R>) -> Result<Length> {
    let mut byte_buf = [0; 1];
    reader.read_exact(&mut byte_buf)?;

    let first_byte = byte_buf[0];
    match first_byte >> 6 {
        0b00 => Ok(Length::EncodedAsInt(decode_length_00(first_byte)?)),
        0b01 => {
            reader.read_exact(&mut byte_buf)?;
            let second_byte = byte_buf[0];
            Ok(Length::EncodedAsInt(
                (decode_length_01([first_byte, second_byte]))?,
            ))
        }
        0b10 => {
            let mut buf = [0; 4];
            reader.read_exact(&mut buf)?;
            Ok(Length::EncodedAsInt(u32::from_le_bytes(buf) as usize))
        }
        0b11 => {
            let remaining_bits = first_byte & 0b0011_1111;
            assert!(remaining_bits <= 2);
            match remaining_bits {
                0 => {
                    let mut buf = [0; 1];
                    reader.read_exact(&mut buf)?;
                    Ok(Length::EncodedAsString(u8::from_le_bytes(buf) as usize))
                }
                1 => {
                    let mut buf = [0; 2];
                    reader.read_exact(&mut buf)?;
                    Ok(Length::EncodedAsString(u16::from_le_bytes(buf) as usize))
                }
                2 => {
                    let mut buf = [0; 4];
                    reader.read_exact(&mut buf)?;
                    Ok(Length::EncodedAsString(u32::from_le_bytes(buf) as usize))
                }
                _ => unreachable!(),
            }
        }
        _ => todo!(),
    }
}

fn decode_string<R: Read>(reader: &mut BufReader<R>) -> Result<String> {
    let length = decode_length(reader)?;

    match length {
        Length::EncodedAsInt(length) => {
            let mut buf = vec![0; length];
            reader.read_exact(&mut buf)?;
            Ok(String::from_utf8(buf)?)
        }
        Length::EncodedAsString(length_str) => Ok(length_str.to_string()),
    }
}

fn decode_value<R: Read>(value_code: u8, reader: &mut BufReader<R>) -> Result<Value> {
    match value_code {
        value_code::STRING => Ok(Value::String(decode_string(reader)?)),
        _ => unimplemented!(),
    }
}

fn decode_key_value<R: Read>(value_code: u8, reader: &mut BufReader<R>) -> Result<(String, Value)> {
    let key = decode_string(reader)?;
    let value = decode_value(value_code, reader)?;
    Ok((key, value))
}

impl Rdb {
    fn read_from_buf<R: Read>(mut f: BufReader<R>) -> Result<Self> {
        let mut read_exact = |n: usize| -> Result<Vec<u8>> {
            let mut buf = vec![0; n];
            f.read_exact(&mut buf)?;
            Ok(buf)
        };

        let mut read_exact_as_string =
            |n| -> Result<String> { Ok(String::from_utf8(read_exact(n)?)?) };

        // Magic String and version number
        let magic = read_exact_as_string(5)?;
        let version = read_exact_as_string(4)?;
        assert_eq!(magic, "REDIS");
        println!("Rdb version: {}", version);

        // Parts
        let mut op_code = [0; 1];
        let store = Store::new();
        while f.read_exact(&mut op_code).is_ok() {
            match op_code[0] {
                AUX => {
                    println!("AUX");
                    let k = decode_string(&mut f)?;
                    let v = decode_string(&mut f)?;
                    println!("Setting: {}:{}", k, v);
                }
                SELECTDB => {
                    println!("SELECTDB");
                    let db = decode_length(&mut f)?.to_usize();
                    println!("Select db: {}", db);
                }
                RESIZEDB => {
                    println!("RESIZEDB");
                    let data_hashtbl_size = decode_length(&mut f)?.to_usize();
                    let expiry_hashtbl_size = decode_length(&mut f)?.to_usize();
                    println!(
                        "Data table size: {}. Expiry table size: {}",
                        data_hashtbl_size, expiry_hashtbl_size
                    );
                }
                EXP_MS => {
                    println!("EXP_MS");
                    let mut buf = [0; 8];
                    f.read_exact(&mut buf)?;
                    let exp = UNIX_EPOCH + Duration::from_millis(u64::from_le_bytes(buf));
                    let curr = SystemTime::now();

                    f.read_exact(&mut op_code)?;
                    let (key, value) = decode_key_value(op_code[0], &mut f)?;
                    println!("KV: {}, {:?}, exp={:?}", key, value, exp);

                    if exp > curr {
                        let exp_in = exp.duration_since(curr)?;
                        store.set(key, value, Some(exp_in));
                    }
                }
                EOF => {
                    println!("EOF");
                    let mut buf = Vec::new();
                    f.read_to_end(&mut buf)?;
                    println!("Checksum: {:?}", buf);
                }
                value_code => {
                    println!("VALUE");

                    let (key, value) = decode_key_value(value_code, &mut f)?;
                    println!("KV: {}, {:?}", key, value);

                    store.set(key, value, None);
                }
            }
        }

        Ok(Self { store })
    }

    pub fn read(path: Option<PathBuf>) -> Result<Self> {
        let empty = Self {
            store: Store::new(),
        };
        match path {
            None => Ok(empty),
            Some(path) => match File::open(path) {
                Ok(f) => {
                    let f = BufReader::new(f);
                    Self::read_from_buf(f)
                }
                Err(err) => {
                    println!("Error opening file: {}", err);
                    Ok(empty)
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine;

    use super::*;

    // Rdb file containing a single key value: 'foo:bar'. Encoded in base64.
    // Obtained by: $ cat FILE | base64
    const SINGLE_KEY_RDB: &str = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjT6CnJlZGlzLWJpdHPAQPoFY3RpbWXCqywlZvoIdXNlZC1tZW3CwIURAPoIYW9mLWJhc2XAAP4A+wEAAANmb28DYmFy/+CZ/pfpGCmk";

    // Rdb file containing 'foo:123' and 'bar:456'. Encoded in base64.
    const MULTI_KEY_RDB: &str ="UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjT6CnJlZGlzLWJpdHPAQPoFY3RpbWXCSlslZvoIdXNlZC1tZW3C8IURAPoIYW9mLWJhc2XAAP4A+wIAAANiYXLByAEAA2Zvb8B7/yfdZT6cKrHT";

    // Rdb file containing 'foo:123', 'bar:456', 'baz:789 with expiration'. Encoded in base64.
    const WITH_EXP_RDB: &str = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjT6CnJlZGlzLWJpdHPAQPoFY3RpbWXCpWQlZvoIdXNlZC1tZW3CoIYRAPoIYW9mLWJhc2XAAP4A+wMBAANmb2/AewADYmFywcgB/PQ1EQKPAQAAAANiYXrBFQP/emKeCGa6hyo=";

    fn single_key_rdb() -> Vec<u8> {
        base64::engine::general_purpose::STANDARD
            .decode(SINGLE_KEY_RDB)
            .unwrap()
    }
    fn multi_key_rdb() -> Vec<u8> {
        base64::engine::general_purpose::STANDARD
            .decode(MULTI_KEY_RDB)
            .unwrap()
    }

    fn with_exp_rdb() -> Vec<u8> {
        base64::engine::general_purpose::STANDARD
            .decode(WITH_EXP_RDB)
            .unwrap()
    }

    fn d(bytes: &[u8]) -> usize {
        decode_length(&mut BufReader::new(bytes))
            .unwrap()
            .to_usize()
    }

    #[test]
    fn test_decode_length() {
        assert_eq!(d(&[0b0000_0000]), 0);
        assert_eq!(d(&[0b0000_0001]), 1);
        assert_eq!(d(&[0b0000_0010]), 2);
        assert_eq!(d(&[0b0010_0000]), 32);
        assert_eq!(d(&[0b0011_1111]), 63);
        assert_eq!(d(&[0b0100_0000, 0b0000_0001]), 64);
        assert_eq!(d(&[0b0100_0001, 0b0000_0001]), 65);
        assert_eq!(d(&[0b0111_1111, 0b1111_1111]), 16383);
        assert_eq!(d(&[0b1000_0000, 0x80, 0x00, 0x00, 0x00]), 128);
        assert_eq!(d(&[0b1000_0000, 0xff, 0xff, 0xff, 0xff]), 4294967295);

        // Additional bytes doesn't matter
        assert_eq!(d(&[0b1000_0000, 0xff, 0xff, 0xff, 0xff, 0xff]), 4294967295);
    }

    #[test]
    fn test_read() {
        let rdb = Rdb::read_from_buf(BufReader::new(&single_key_rdb()[..])).unwrap();
        assert_eq!(rdb.store.data().len(), 1);
        assert_eq!(rdb.store.get("foo").unwrap().to_string(), "bar");

        let rdb = Rdb::read_from_buf(BufReader::new(&multi_key_rdb()[..])).unwrap();
        assert_eq!(rdb.store.data().len(), 2);
        assert_eq!(rdb.store.get("foo").unwrap().to_string(), "123");
        assert_eq!(rdb.store.get("bar").unwrap().to_string(), "456");
    }

    #[test]
    fn test_read_exp() {
        let rdb = Rdb::read_from_buf(BufReader::new(&(with_exp_rdb())[..])).unwrap();
        assert_eq!(rdb.store.data().len(), 2);
        assert_eq!(rdb.store.get("foo").unwrap().to_string(), "123");
        assert_eq!(rdb.store.get("bar").unwrap().to_string(), "456");
    }
}
