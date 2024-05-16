use crate::{extract_simple_frame_data, RespDecode, RespEncode, RespError, CRLF_LEN};
use bytes::BytesMut;
use std::ops::Deref;

#[derive(Debug, Clone, PartialEq)]
pub struct SimpleError(pub(crate) String);

impl RespEncode for SimpleError {
    fn encode(self) -> Vec<u8> {
        format!("-{}\r\n", self.0).into_bytes()
    }
}

impl RespDecode for SimpleError {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let prefix = "-";
        let end = extract_simple_frame_data(buf, prefix, 1)?;

        let data = buf.split_to(end + CRLF_LEN);
        let s = String::from_utf8_lossy(&data[prefix.len()..end]).to_string();
        Ok(SimpleError::new(s))
    }
}

impl Deref for SimpleError {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SimpleError {
    pub fn new(s: impl Into<String>) -> Self {
        SimpleError(s.into())
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_simple_error_encode() {
        let frame: RespFrame = SimpleError::new("ERR").into();

        assert_eq!(frame.encode(), b"-ERR\r\n");
    }

    #[test]
    fn test_simple_error_decode() -> Result<()> {
        let mut buf = BytesMut::from("-ERR\r\n");
        let frame = SimpleError::decode(&mut buf)?;
        assert_eq!(frame, SimpleError::new("ERR".to_string()));

        Ok(())
    }
}
