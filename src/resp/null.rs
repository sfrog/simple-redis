use crate::{extract_fixed_data, RespDecode, RespEncode, RespError};
use bytes::BytesMut;

#[derive(Debug, Clone, PartialEq)]
pub struct RespNull;

impl RespEncode for RespNull {
    fn encode(self) -> Vec<u8> {
        b"_\r\n".to_vec()
    }
}

impl RespDecode for RespNull {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extract_fixed_data(buf, "_\r\n")?;
        Ok(RespNull)
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_null_encode() {
        let frame: RespFrame = RespNull.into();
        assert_eq!(frame.encode(), b"_\r\n");
    }

    #[test]
    fn test_null_decode() -> Result<()> {
        let mut buf = BytesMut::from("_\r\n");
        let frame = RespNull::decode(&mut buf)?;
        assert_eq!(frame, RespNull);

        Ok(())
    }
}
