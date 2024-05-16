use crate::{
    extract_fixed_data, extract_simple_frame_data, parse_length, RespDecode, RespEncode, RespError,
    CRLF_LEN,
};
use bytes::{Buf, BytesMut};

#[derive(Debug, Clone, PartialEq)]
pub struct BulkString(pub(crate) Option<Vec<u8>>);

impl RespEncode for BulkString {
    fn encode(self) -> Vec<u8> {
        match self.0 {
            Some(data) => {
                let mut buf = Vec::with_capacity(data.len() + 16);
                buf.extend_from_slice(&format!("${}\r\n", data.len()).into_bytes());
                buf.extend_from_slice(&data);
                buf.extend_from_slice(b"\r\n");
                buf
            }
            None => b"$-1\r\n".to_vec(),
        }
    }
}

impl RespDecode for BulkString {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        if extract_fixed_data(buf, "$-1\r\n").is_ok() {
            return Ok(BulkString(None));
        }
        let prefix = "$";
        let end = extract_simple_frame_data(buf, prefix, 2)?;
        let mut data = buf.split_to(end + CRLF_LEN);

        let (end, len) = parse_length(&data, prefix)?;
        if len < 0 {
            return Err(RespError::InvalidFrame(
                "Invalid bulk string length".to_string(),
            ));
        }

        let len = len as usize;

        data.advance(end + CRLF_LEN);

        if data.len() != len + 2 {
            return Err(RespError::NotComplete);
        }

        Ok(BulkString::new(data[0..len].to_vec()))
    }
}

impl BulkString {
    pub fn new(s: impl Into<Vec<u8>>) -> Self {
        BulkString(Some(s.into()))
    }
    pub fn new_null() -> Self {
        BulkString(None)
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use anyhow::Result;
    use bytes::{BufMut, BytesMut};

    #[test]
    fn test_bulk_string_encode() {
        let frame: RespFrame = BulkString::new(b"hello".to_vec()).into();
        assert_eq!(frame.encode(), b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::from("$5\r\nhello\r\n");
        let frame = BulkString::decode(&mut buf)?;
        assert_eq!(frame, BulkString::new(b"hello".to_vec()));

        buf.extend_from_slice("$5\r\nhello\r".as_bytes());
        let frame = BulkString::decode(&mut buf);
        assert_eq!(frame.unwrap_err(), RespError::NotComplete);

        buf.put_u8(b'\n');
        let frame = BulkString::decode(&mut buf)?;
        assert_eq!(frame, BulkString::new(b"hello".to_vec()));

        Ok(())
    }

    #[test]
    fn test_null_bulk_string_encode() {
        let frame: RespFrame = BulkString::new_null().into();
        assert_eq!(frame.encode(), b"$-1\r\n");
    }

    #[test]
    fn test_null_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::from("$-1\r\n");
        let frame = BulkString::decode(&mut buf)?;
        assert_eq!(frame, BulkString::new_null());

        Ok(())
    }
}
