use crate::{extract_simple_frame_data, RespDecode, RespEncode, RespError, CRLF_LEN};
use bytes::BytesMut;

impl RespDecode for f64 {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let prefix = ",";
        let end = extract_simple_frame_data(buf, prefix, 1)?;

        let data = buf.split_to(end + CRLF_LEN);
        let s = String::from_utf8_lossy(&data[prefix.len()..end]).to_string();
        Ok(s.parse()?)
    }
}

impl RespEncode for f64 {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(32);
        let ret = if self.abs() > 1e+8 || self.abs() < 1e-8 {
            format!(",{:+e}\r\n", self)
        } else {
            let sign = if self < 0.0 { "" } else { "+" };
            format!(",{}{}\r\n", sign, self)
        };
        buf.extend_from_slice(&ret.into_bytes());
        buf
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use anyhow::Result;
    use bytes::BytesMut;

    #[test]
    fn test_double_encode() {
        let frame: RespFrame = 123.456.into();
        assert_eq!(frame.encode(), b",+123.456\r\n");

        let frame: RespFrame = (-123.456).into();
        assert_eq!(frame.encode(), b",-123.456\r\n");

        let frame: RespFrame = 1.23456789e+9.into();
        assert_eq!(frame.encode(), b",+1.23456789e9\r\n");

        let frame: RespFrame = (-1.23456789e-9).into();
        assert_eq!(frame.encode(), b",-1.23456789e-9\r\n");
    }

    #[test]
    fn test_float_decode() -> Result<()> {
        let mut buf = BytesMut::from(",5.14\r\n");
        let frame = f64::decode(&mut buf)?;
        assert_eq!(frame, 5.14);

        let mut buf = BytesMut::from(",-5.14\r\n");
        let frame = f64::decode(&mut buf)?;
        assert_eq!(frame, -5.14);

        let mut buf = BytesMut::from(",-5.14e9\r\n");
        let frame = f64::decode(&mut buf)?;
        assert_eq!(frame, -5.14e9);

        let mut buf = BytesMut::from(",5.14e-9\r\n");
        let frame = f64::decode(&mut buf)?;
        assert_eq!(frame, 5.14e-9);
        Ok(())
    }
}
