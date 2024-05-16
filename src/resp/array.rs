use std::ops::Deref;

use bytes::{Buf, BytesMut};

use crate::{
    extract_fixed_data, parse_length, RespDecode, RespEncode, RespError, RespFrame, BUF_CAPACITY,
    CRLF_LEN,
};

#[derive(Debug, Clone, PartialEq)]
pub struct RespArray(pub(crate) Vec<RespFrame>);

#[derive(Debug, Clone, PartialEq)]
pub struct RespNullArray;

impl RespEncode for RespArray {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BUF_CAPACITY);
        buf.extend_from_slice(&format!("*{}\r\n", self.len()).into_bytes());
        for frame in self.0 {
            buf.extend_from_slice(&frame.encode());
        }
        buf
    }
}

impl RespDecode for RespArray {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let prefix = "*";
        let (end, len) = parse_length(buf, prefix)?;

        // do with the cloned buffer
        let mut try_buf = buf.clone();
        try_buf.advance(end + CRLF_LEN);

        let mut frames = Vec::with_capacity(len);
        for _ in 0..len {
            if try_buf.is_empty() {
                return Err(RespError::NotComplete);
            }
            frames.push(RespFrame::decode(&mut try_buf)?);
        }

        // if all frames are decoded successfully, update the original buffer
        *buf = try_buf;

        Ok(RespArray::new(frames))
    }
}

impl RespEncode for RespNullArray {
    fn encode(self) -> Vec<u8> {
        b"*-1\r\n".to_vec()
    }
}

impl RespDecode for RespNullArray {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extract_fixed_data(buf, "*-1\r\n")?;
        Ok(RespNullArray)
    }
}

impl Deref for RespArray {
    type Target = Vec<RespFrame>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl RespArray {
    pub fn new(v: Vec<RespFrame>) -> Self {
        RespArray(v)
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_array_encode() {
        let frame: RespFrame = RespArray::new(vec![
            SimpleString::new("OK").into(),
            SimpleError::new("ERR").into(),
            123.into(),
            BulkString::new(b"hello".to_vec()).into(),
            RespNullBulkString.into(),
            RespNullArray.into(),
        ])
        .into();
        assert_eq!(
            frame.encode(),
            b"*6\r\n+OK\r\n-ERR\r\n:+123\r\n$5\r\nhello\r\n$-1\r\n*-1\r\n"
        );
    }

    #[test]
    fn test_array_decode() -> Result<()> {
        let mut buf = BytesMut::from("*3\r\n+OK\r\n-ERR\r\n:123\r\n");
        let frame = RespArray::decode(&mut buf)?;
        let array = RespArray::new(vec![
            SimpleString::new("OK").into(),
            SimpleError::new("ERR").into(),
            123.into(),
        ]);
        assert_eq!(frame, array);

        buf.extend_from_slice("*3\r\n+OK\r\n-ERR\r\n".as_bytes());
        let frame = RespArray::decode(&mut buf);
        assert_eq!(frame.unwrap_err(), RespError::NotComplete);

        buf.extend_from_slice(":123\r\n".as_bytes());
        let frame = RespArray::decode(&mut buf)?;
        let array = RespArray::new(vec![
            SimpleString::new("OK").into(),
            SimpleError::new("ERR").into(),
            123.into(),
        ]);
        assert_eq!(frame, array);
        Ok(())
    }

    #[test]
    fn test_null_array_encode() {
        let frame: RespFrame = RespNullArray.into();
        assert_eq!(frame.encode(), b"*-1\r\n");
    }

    #[test]
    fn test_null_array_decode() -> Result<()> {
        let mut buf = BytesMut::from("*-1\r\n");
        let frame = RespNullArray::decode(&mut buf)?;
        assert_eq!(frame, RespNullArray);

        Ok(())
    }
}
