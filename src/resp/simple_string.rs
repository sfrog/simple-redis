use crate::{extract_simple_frame_data, RespDecode, RespEncode, RespError, CRLF_LEN};
use bytes::BytesMut;
use std::ops::Deref;

#[derive(Debug, Clone, PartialEq)]
pub struct SimpleString(pub(crate) String);

impl RespDecode for SimpleString {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let prefix = "+";
        let end = extract_simple_frame_data(buf, prefix, 1)?;

        let data = buf.split_to(end + CRLF_LEN);
        let s = String::from_utf8_lossy(&data[prefix.len()..end]).to_string();
        Ok(SimpleString::new(s))
    }
}

impl RespEncode for SimpleString {
    fn encode(self) -> Vec<u8> {
        format!("+{}\r\n", self.0).into_bytes()
    }
}

impl Deref for SimpleString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SimpleString {
    pub fn new(s: impl Into<String>) -> Self {
        SimpleString(s.into())
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use anyhow::Result;
    use bytes::{BufMut, BytesMut};

    #[test]
    fn test_simple_string_encode() {
        let frame: RespFrame = SimpleString::new("OK").into();

        assert_eq!(frame.encode(), b"+OK\r\n");
    }

    #[test]
    fn test_simple_string_decode() -> Result<()> {
        let mut buf = BytesMut::from("+OK\r\n");
        let frame = SimpleString::decode(&mut buf)?;
        assert_eq!(frame, SimpleString::new("OK".to_string()));

        buf.extend_from_slice("+hello\r".as_bytes());
        let frame = SimpleString::decode(&mut buf);
        assert_eq!(frame.unwrap_err(), RespError::NotComplete);

        buf.put_u8(b'\n');
        let frame = SimpleString::decode(&mut buf)?;
        assert_eq!(frame, SimpleString::new("hello".to_string()));

        Ok(())
    }
}
