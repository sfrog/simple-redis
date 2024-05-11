use crate::{
    BulkString, RespArray, RespEncode, RespMap, RespNull, RespNullArray, RespNullBulkString,
    RespSet, SimpleError, SimpleString,
};

const BUF_CAPACITY: usize = 4096;

impl RespEncode for SimpleString {
    fn encode(self) -> Vec<u8> {
        format!("+{}\r\n", self.0).into_bytes()
    }
}

impl RespEncode for SimpleError {
    fn encode(self) -> Vec<u8> {
        format!("-{}\r\n", self.0).into_bytes()
    }
}

impl RespEncode for i64 {
    fn encode(self) -> Vec<u8> {
        let sign = if self < 0 { "" } else { "+" };
        format!(":{}{}\r\n", sign, self).into_bytes()
    }
}

impl RespEncode for BulkString {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.len() + 16);
        buf.extend_from_slice(&format!("${}\r\n", self.len()).into_bytes());
        buf.extend_from_slice(&self);
        buf.extend_from_slice(b"\r\n");
        buf
    }
}

impl RespEncode for RespNullBulkString {
    fn encode(self) -> Vec<u8> {
        b"$-1\r\n".to_vec()
    }
}

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

impl RespEncode for RespNull {
    fn encode(self) -> Vec<u8> {
        b"_\r\n".to_vec()
    }
}

impl RespEncode for RespNullArray {
    fn encode(self) -> Vec<u8> {
        b"*-1\r\n".to_vec()
    }
}

impl RespEncode for bool {
    fn encode(self) -> Vec<u8> {
        format!("#{}\r\n", if self { "t" } else { "f" }).into_bytes()
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

#[cfg(test)]
mod tests {
    use crate::RespFrame;

    use super::*;

    #[test]
    fn test_simple_string_encode() {
        let frame: RespFrame = SimpleString::new("OK").into();

        assert_eq!(frame.encode(), b"+OK\r\n");
    }

    #[test]
    fn test_simple_error_encode() {
        let frame: RespFrame = SimpleError::new("ERR").into();

        assert_eq!(frame.encode(), b"-ERR\r\n");
    }

    #[test]
    fn test_integer_encode() {
        let frame: RespFrame = 123.into();
        assert_eq!(frame.encode(), b":+123\r\n");

        let frame: RespFrame = (-123).into();
        assert_eq!(frame.encode(), b":-123\r\n");
    }

    #[test]
    fn test_bulk_string_encode() {
        let frame: RespFrame = BulkString::new(b"hello".to_vec()).into();
        assert_eq!(frame.encode(), b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_null_bulk_string_encode() {
        let frame: RespFrame = RespNullBulkString.into();
        assert_eq!(frame.encode(), b"$-1\r\n");
    }

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
    fn test_null_encode() {
        let frame: RespFrame = RespNull.into();
        assert_eq!(frame.encode(), b"_\r\n");
    }

    #[test]
    fn test_null_array_encode() {
        let frame: RespFrame = RespNullArray.into();
        assert_eq!(frame.encode(), b"*-1\r\n");
    }

    #[test]
    fn test_bool_encode() {
        let frame: RespFrame = true.into();
        assert_eq!(frame.encode(), b"#t\r\n");

        let frame: RespFrame = false.into();
        assert_eq!(frame.encode(), b"#f\r\n");
    }

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
    fn test_set_encode() {
        let frame: RespFrame = RespSet::new(vec![
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
            b"~6\r\n+OK\r\n-ERR\r\n:+123\r\n$5\r\nhello\r\n$-1\r\n*-1\r\n"
        );
    }
}
