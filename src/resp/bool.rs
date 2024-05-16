use crate::{extract_fixed_data, RespDecode, RespEncode, RespError};
use bytes::BytesMut;

impl RespEncode for bool {
    fn encode(self) -> Vec<u8> {
        format!("#{}\r\n", if self { "t" } else { "f" }).into_bytes()
    }
}

impl RespDecode for bool {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        match extract_fixed_data(buf, "#t\r\n") {
            Ok(_) => Ok(true),
            Err(_) => match extract_fixed_data(buf, "#f\r\n") {
                Ok(_) => Ok(false),
                Err(_) => Err(RespError::InvalidFrameType("Invalid boolean".to_string())),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_bool_encode() {
        let frame: RespFrame = true.into();
        assert_eq!(frame.encode(), b"#t\r\n");

        let frame: RespFrame = false.into();
        assert_eq!(frame.encode(), b"#f\r\n");
    }

    #[test]
    fn test_boolean_decode() -> Result<()> {
        let mut buf = BytesMut::from("#t\r\n");
        let frame = bool::decode(&mut buf)?;
        assert!(frame);

        buf = BytesMut::from("#f\r\n");
        let frame = bool::decode(&mut buf)?;
        assert!(!frame);

        Ok(())
    }
}
