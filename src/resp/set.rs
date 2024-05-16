use crate::{parse_length, RespDecode, RespEncode, RespError, RespFrame, BUF_CAPACITY, CRLF_LEN};
use bytes::{Buf, BytesMut};
use std::ops::Deref;

#[derive(Debug, Clone, PartialEq)]
pub struct RespSet(Vec<RespFrame>);

impl RespEncode for RespSet {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BUF_CAPACITY);
        buf.extend_from_slice(&format!("~{}\r\n", self.len()).into_bytes());
        for frame in self.0 {
            buf.extend_from_slice(&frame.encode());
        }
        buf
    }
}

impl RespDecode for RespSet {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let prefix = "~";
        let (end, len) = parse_length(buf, prefix)?;

        // do with the cloned buffer
        let mut try_buf = buf.clone();
        try_buf.advance(end + CRLF_LEN);

        let mut frames = Vec::new();
        for _ in 0..len {
            if try_buf.is_empty() {
                return Err(RespError::NotComplete);
            }
            frames.push(RespFrame::decode(&mut try_buf)?);
        }

        // if all frames are decoded successfully, update the original buffer
        *buf = try_buf;

        Ok(RespSet::new(frames))
    }
}

impl Deref for RespSet {
    type Target = Vec<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl RespSet {
    pub fn new(s: impl Into<Vec<RespFrame>>) -> Self {
        let s = s.into();
        RespSet(s)
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_set_encode() {
        let frame: RespFrame = RespSet::new(vec![
            SimpleString::new("OK").into(),
            SimpleError::new("ERR").into(),
            123.into(),
            BulkString::new(b"hello".to_vec()).into(),
            BulkString::new_null().into(),
            RespArray::new_null().into(),
        ])
        .into();
        assert_eq!(
            frame.encode(),
            b"~6\r\n+OK\r\n-ERR\r\n:123\r\n$5\r\nhello\r\n$-1\r\n*-1\r\n"
        );
    }

    #[test]
    fn test_set_decode() -> Result<()> {
        let mut buf = BytesMut::from("~3\r\n+OK\r\n-ERR\r\n:123\r\n");
        let frame = RespSet::decode(&mut buf)?;
        let set = RespSet::new(vec![
            SimpleString::new("OK").into(),
            SimpleError::new("ERR").into(),
            123.into(),
        ]);
        assert_eq!(frame, set);

        buf.extend_from_slice("~3\r\n+OK\r\n-ERR\r\n".as_bytes());
        let frame = RespSet::decode(&mut buf);
        assert_eq!(frame.unwrap_err(), RespError::NotComplete);

        buf.extend_from_slice(":123\r\n".as_bytes());
        let frame = RespSet::decode(&mut buf)?;
        let set = RespSet::new(vec![
            SimpleString::new("OK").into(),
            SimpleError::new("ERR").into(),
            123.into(),
        ]);
        assert_eq!(frame, set);
        Ok(())
    }
}
