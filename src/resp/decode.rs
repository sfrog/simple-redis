use crate::{
    BulkString, RespArray, RespDecode, RespError, RespFrame, RespMap, RespNull, RespNullArray,
    RespNullBulkString, RespSet, SimpleError, SimpleString,
};
use bytes::{Buf, BytesMut};

const CRLF_LEN: usize = 2;

impl RespDecode for RespFrame {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let mut iter = buf.iter().peekable();
        match iter.peek() {
            Some(b'+') => SimpleString::decode(buf).map(RespFrame::SimpleString),
            Some(b'-') => SimpleError::decode(buf).map(RespFrame::Error),
            Some(b':') => i64::decode(buf).map(RespFrame::Integer),
            Some(b'$') => match RespNullBulkString::decode(buf) {
                Ok(frame) => Ok(RespFrame::NullBulkString(frame)),
                Err(_) => BulkString::decode(buf).map(RespFrame::BulkString),
            },
            Some(b'*') => match RespNullArray::decode(buf) {
                Ok(frame) => Ok(RespFrame::NullArray(frame)),
                Err(_) => RespArray::decode(buf).map(RespFrame::Array),
            },
            Some(b'_') => RespNull::decode(buf).map(RespFrame::Null),
            Some(b'#') => bool::decode(buf).map(RespFrame::Boolean),
            Some(b',') => f64::decode(buf).map(RespFrame::Double),
            Some(b'%') => RespMap::decode(buf).map(RespFrame::Map),
            Some(b'~') => RespSet::decode(buf).map(RespFrame::Set),
            _ => Err(RespError::InvalidFrameType(format!(
                "Invalid frame type: {:?}",
                buf
            ))),
        }
    }
}

impl RespDecode for SimpleString {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let prefix = "+";
        let end = extract_simple_frame_date(buf, prefix, 1)?;

        let data = buf.split_to(end + CRLF_LEN);
        let s = String::from_utf8_lossy(&data[prefix.len()..end]).to_string();
        Ok(SimpleString::new(s))
    }
}

impl RespDecode for SimpleError {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let prefix = "-";
        let end = extract_simple_frame_date(buf, prefix, 1)?;

        let data = buf.split_to(end + CRLF_LEN);
        let s = String::from_utf8_lossy(&data[prefix.len()..end]).to_string();
        Ok(SimpleError::new(s))
    }
}

impl RespDecode for i64 {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let prefix = ":";
        let end = extract_simple_frame_date(buf, prefix, 1)?;

        let data = buf.split_to(end + CRLF_LEN);
        let s = String::from_utf8_lossy(&data[prefix.len()..end]).to_string();
        Ok(s.parse()?)
    }
}

impl RespDecode for RespNull {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extract_fixed_data(buf, "_\r\n")?;
        Ok(RespNull)
    }
}

impl RespDecode for RespNullArray {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extract_fixed_data(buf, "*-1\r\n")?;
        Ok(RespNullArray)
    }
}

impl RespDecode for RespNullBulkString {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extract_fixed_data(buf, "$-1\r\n")?;
        Ok(RespNullBulkString)
    }
}

impl RespDecode for BulkString {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let prefix = "$";
        let end = extract_simple_frame_date(buf, prefix, 2)?;
        let mut data = buf.split_to(end + CRLF_LEN);

        let (end, len) = parse_length(&data, prefix)?;
        data.advance(end + CRLF_LEN);

        if data.len() != len + 2 {
            return Err(RespError::NotComplete);
        }

        Ok(BulkString::new(data[0..len].to_vec()))
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

impl RespDecode for f64 {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let prefix = ",";
        let end = extract_simple_frame_date(buf, prefix, 1)?;

        let data = buf.split_to(end + CRLF_LEN);
        let s = String::from_utf8_lossy(&data[prefix.len()..end]).to_string();
        Ok(s.parse()?)
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

impl RespDecode for RespMap {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let prefix = "%";
        let (end, len) = parse_length(buf, prefix)?;

        // do with the cloned buffer
        let mut try_buf = buf.clone();
        try_buf.advance(end + CRLF_LEN);

        let mut frames = RespMap::new();
        for _ in 0..len {
            if try_buf.is_empty() {
                return Err(RespError::NotComplete);
            }
            let key = SimpleString::decode(&mut try_buf)?;
            if try_buf.is_empty() {
                return Err(RespError::NotComplete);
            }
            let value = RespFrame::decode(&mut try_buf)?;
            frames.insert(key.0, value);
        }

        // if all frames are decoded successfully, update the original buffer
        *buf = try_buf;

        Ok(frames)
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

fn extract_fixed_data(buf: &mut BytesMut, expect: &str) -> Result<(), RespError> {
    if buf.len() < expect.len() {
        return Err(RespError::NotComplete);
    }

    if !buf.starts_with(expect.as_bytes()) {
        return Err(RespError::InvalidFrameType(format!(
            "Expecting '{}', got {:?}",
            expect, buf
        )));
    }

    buf.advance(expect.len());
    Ok(())
}

fn extract_simple_frame_date(
    buf: &BytesMut,
    prefix: &str,
    nth_crlf: usize,
) -> Result<usize, RespError> {
    if buf.len() < 3 {
        return Err(RespError::NotComplete);
    }

    if !buf.starts_with(prefix.as_bytes()) {
        return Err(RespError::InvalidFrameType(format!(
            "Expecting '{}', got {:?}",
            prefix, buf
        )));
    }

    let end = find_crlf(buf, nth_crlf).ok_or(RespError::NotComplete)?;

    if end == 0 {
        return Err(RespError::NotComplete);
    }

    Ok(end)
}

fn find_crlf(buf: &BytesMut, nth: usize) -> Option<usize> {
    let mut count = 0;
    for (i, &c) in buf.iter().enumerate() {
        if c == b'\r' {
            if let Some(b'\n') = buf.get(i + 1) {
                count += 1;
                if count == nth {
                    return Some(i);
                }
            }
        }
    }
    None
}

fn parse_length(buf: &BytesMut, prefix: &str) -> Result<(usize, usize), RespError> {
    let end = extract_simple_frame_date(buf, prefix, 1)?;
    let s = String::from_utf8_lossy(&buf[prefix.len()..end]);
    Ok((end, s.parse()?))
}

// fn calc_total_length(buf: &BytesMut, prefix: &str) -> Result<usize, RespError> {
//     let (end, len) = parse_length(buf, prefix)?;
//     match prefix {
//         "*" | "~" => {}
//         "%" => {
//             let mut total = 0;
//             let mut iter = buf.iter().skip(end + 2);
//             for _ in 0..len {
//                 let key_end = find_crlf(&buf, 1).ok_or(RespError::NotComplete)?;
//                 let key_len = key_end - end - 2;
//                 total += key_len + 2;
//                 iter.advance(key_len + 2);
//                 total += RespFrame::decode(&mut iter.collect())?.encode().len();
//             }
//             Ok(total)
//         }
//         _ => Err(RespError::InvalidFrameType(
//             "Invalid frame type".to_string(),
//         )),
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use bytes::BufMut;

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

    #[test]
    fn test_simple_error_decode() -> Result<()> {
        let mut buf = BytesMut::from("-ERR\r\n");
        let frame = SimpleError::decode(&mut buf)?;
        assert_eq!(frame, SimpleError::new("ERR".to_string()));

        Ok(())
    }

    #[test]
    fn test_integer_decode() -> Result<()> {
        let mut buf = BytesMut::from(":123\r\n");
        let frame = i64::decode(&mut buf)?;
        assert_eq!(frame, 123);

        buf.extend_from_slice(":-100\r\n".as_bytes());
        let frame = i64::decode(&mut buf)?;
        assert_eq!(frame, -100);

        Ok(())
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
    fn test_null_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::from("$-1\r\n");
        let frame = RespNullBulkString::decode(&mut buf)?;
        assert_eq!(frame, RespNullBulkString);

        Ok(())
    }

    #[test]
    fn test_null_array_decode() -> Result<()> {
        let mut buf = BytesMut::from("*-1\r\n");
        let frame = RespNullArray::decode(&mut buf)?;
        assert_eq!(frame, RespNullArray);

        Ok(())
    }

    #[test]
    fn test_null_decode() -> Result<()> {
        let mut buf = BytesMut::from("_\r\n");
        let frame = RespNull::decode(&mut buf)?;
        assert_eq!(frame, RespNull);

        Ok(())
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

    #[test]
    fn test_map_decode() -> Result<()> {
        let mut buf = BytesMut::from("%2\r\n+key1\r\n:123\r\n+key2\r\n$5\r\nhello\r\n");
        let frame = RespMap::decode(&mut buf)?;
        let mut map = RespMap::new();
        map.insert("key1".to_string(), 123.into());
        map.insert(
            "key2".to_string(),
            BulkString::new(b"hello".to_vec()).into(),
        );
        assert_eq!(frame, map);

        buf.extend_from_slice("%2\r\n+key1\r\n:123\r\n+key2\r\n".as_bytes());
        let frame = RespMap::decode(&mut buf);
        assert_eq!(frame.unwrap_err(), RespError::NotComplete);

        buf.extend_from_slice("$5\r\nhello\r\n".as_bytes());
        let frame = RespMap::decode(&mut buf)?;
        let mut map = RespMap::new();
        map.insert("key1".to_string(), 123.into());
        map.insert(
            "key2".to_string(),
            BulkString::new(b"hello".to_vec()).into(),
        );
        assert_eq!(frame, map);
        Ok(())
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
}
