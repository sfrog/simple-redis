use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut},
};

use bytes::{Buf, BytesMut};

use crate::{
    parse_length, RespDecode, RespEncode, RespError, RespFrame, SimpleString, BUF_CAPACITY,
    CRLF_LEN,
};

#[derive(Debug, Clone, PartialEq)]
pub struct RespMap(BTreeMap<String, RespFrame>);

impl RespEncode for RespMap {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BUF_CAPACITY);
        buf.extend_from_slice(&format!("%{}\r\n", self.len()).into_bytes());
        for (k, v) in self.0 {
            buf.extend_from_slice(&SimpleString::new(k).encode());
            buf.extend_from_slice(&v.encode());
        }
        buf
    }
}

impl RespDecode for RespMap {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let prefix = "%";
        let (end, len) = parse_length(buf, prefix)?;

        // do with the cloned buffer
        let mut try_buf = buf.clone();
        try_buf.advance(end + CRLF_LEN);

        let mut frames = RespMap::new();
        for _ in 0..len {
            if try_buf.is_empty() {
                return Err(RespError::NotComplete);
            }
            let key = SimpleString::decode(&mut try_buf)?;
            if try_buf.is_empty() {
                return Err(RespError::NotComplete);
            }
            let value = RespFrame::decode(&mut try_buf)?;
            frames.insert(key.0, value);
        }

        // if all frames are decoded successfully, update the original buffer
        *buf = try_buf;

        Ok(frames)
    }
}

impl Deref for RespMap {
    type Target = BTreeMap<String, RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RespMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl RespMap {
    pub fn new() -> Self {
        RespMap(BTreeMap::new())
    }
}

impl Default for RespMap {
    fn default() -> Self {
        RespMap::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_map_encode() {
        let mut map = RespMap::new();
        map.insert("key01".to_string(), SimpleString::new("value1").into());
        map.insert("key02".to_string(), SimpleError::new("value2").into());
        map.insert("key03".to_string(), 123.into());
        map.insert(
            "key04".to_string(),
            BulkString::new(b"value4".to_vec()).into(),
        );
        map.insert("key05".to_string(), RespNullBulkString.into());
        map.insert("key06".to_string(), RespNullArray.into());
        map.insert("key07".to_string(), true.into());
        map.insert("key08".to_string(), false.into());
        map.insert("key09".to_string(), 123.456.into());
        map.insert("key10".to_string(), (-123.456).into());
        map.insert("key11".to_string(), 1.23456789e+9.into());
        map.insert("key12".to_string(), (-1.23456789e-9).into());
        map.insert("key13".to_string(), RespArray::new(vec![]).into());
        map.insert("key14".to_string(), RespMap::new().into());
        map.insert("key15".to_string(), RespSet::new(vec![]).into());

        let frame: RespFrame = map.into();
        assert_eq!(
            frame.encode(),
            b"%15\r\n\
            +key01\r\n+value1\r\n\
            +key02\r\n-value2\r\n\
            +key03\r\n:+123\r\n\
            +key04\r\n$6\r\nvalue4\r\n\
            +key05\r\n$-1\r\n\
            +key06\r\n*-1\r\n\
            +key07\r\n#t\r\n\
            +key08\r\n#f\r\n\
            +key09\r\n,+123.456\r\n\
            +key10\r\n,-123.456\r\n\
            +key11\r\n,+1.23456789e9\r\n\
            +key12\r\n,-1.23456789e-9\r\n\
            +key13\r\n*0\r\n\
            +key14\r\n%0\r\n\
            +key15\r\n~0\r\n"
        );
    }

    #[test]
    fn test_map_decode() -> Result<()> {
        let mut buf = BytesMut::from("%2\r\n+key1\r\n:123\r\n+key2\r\n$5\r\nhello\r\n");
        let frame = RespMap::decode(&mut buf)?;
        let mut map = RespMap::new();
        map.insert("key1".to_string(), 123.into());
        map.insert(
            "key2".to_string(),
            BulkString::new(b"hello".to_vec()).into(),
        );
        assert_eq!(frame, map);

        buf.extend_from_slice("%2\r\n+key1\r\n:123\r\n+key2\r\n".as_bytes());
        let frame = RespMap::decode(&mut buf);
        assert_eq!(frame.unwrap_err(), RespError::NotComplete);

        buf.extend_from_slice("$5\r\nhello\r\n".as_bytes());
        let frame = RespMap::decode(&mut buf)?;
        let mut map = RespMap::new();
        map.insert("key1".to_string(), 123.into());
        map.insert(
            "key2".to_string(),
            BulkString::new(b"hello".to_vec()).into(),
        );
        assert_eq!(frame, map);
        Ok(())
    }
}
