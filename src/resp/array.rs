use crate::{
    extract_fixed_data, parse_length, RespDecode, RespEncode, RespError, RespFrame, BUF_CAPACITY,
    CRLF_LEN,
};
use bytes::{Buf, BytesMut};

#[derive(Debug, Clone, PartialEq)]
pub struct RespArray(pub(crate) Option<Vec<RespFrame>>);

impl RespEncode for RespArray {
    fn encode(self) -> Vec<u8> {
        match self {
            RespArray(None) => b"*-1\r\n".to_vec(),
            RespArray(Some(v)) => {
                let mut buf = Vec::with_capacity(BUF_CAPACITY);
                buf.extend_from_slice(&format!("*{}\r\n", v.len()).into_bytes());
                for frame in v {
                    buf.extend_from_slice(&frame.encode());
                }
                buf
            }
        }
    }
}

impl RespDecode for RespArray {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        if extract_fixed_data(buf, "*-1\r\n").is_ok() {
            return Ok(RespArray(None));
        }

        let prefix = "*";
        let (end, len) = parse_length(buf, prefix)?;

        if len < 0 {
            return Err(RespError::InvalidFrame("Invalid array length".to_string()));
        }

        let len = len as usize;

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

impl RespArray {
    pub fn new(v: Vec<RespFrame>) -> Self {
        RespArray(Some(v))
    }

    pub fn new_null() -> Self {
        RespArray(None)
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
            BulkString::new_null().into(),
            RespArray::new_null().into(),
        ])
        .into();
        assert_eq!(
            frame.encode(),
            b"*6\r\n+OK\r\n-ERR\r\n:123\r\n$5\r\nhello\r\n$-1\r\n*-1\r\n"
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
        let frame: RespFrame = RespArray::new_null().into();
        assert_eq!(frame.encode(), b"*-1\r\n");
    }

    #[test]
    fn test_null_array_decode() -> Result<()> {
        let mut buf = BytesMut::from("*-1\r\n");
        let frame = RespArray::decode(&mut buf)?;
        assert_eq!(frame, RespArray::new_null());

        Ok(())
    }
}
