use bytes::BytesMut;
use enum_dispatch::enum_dispatch;

use crate::{
    BulkString, RespArray, RespDecode, RespError, RespMap, RespNull, RespSet, SimpleError,
    SimpleString,
};

#[enum_dispatch(RespEncode)]
#[derive(Debug, Clone, PartialEq)]
pub enum RespFrame {
    SimpleString(SimpleString),
    Error(SimpleError),
    Integer(i64),
    BulkString(BulkString),
    Array(RespArray),
    Null(RespNull),
    Boolean(bool),
    Double(f64),
    Map(RespMap),
    Set(RespSet),
}

impl RespDecode for RespFrame {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let mut iter = buf.iter().peekable();
        match iter.peek() {
            Some(b'+') => SimpleString::decode(buf).map(RespFrame::SimpleString),
            Some(b'-') => SimpleError::decode(buf).map(RespFrame::Error),
            Some(b':') => i64::decode(buf).map(RespFrame::Integer),
            Some(b'$') => BulkString::decode(buf).map(RespFrame::BulkString),
            Some(b'*') => RespArray::decode(buf).map(RespFrame::Array),
            Some(b'_') => RespNull::decode(buf).map(RespFrame::Null),
            Some(b'#') => bool::decode(buf).map(RespFrame::Boolean),
            Some(b',') => f64::decode(buf).map(RespFrame::Double),
            Some(b'%') => RespMap::decode(buf).map(RespFrame::Map),
            Some(b'~') => RespSet::decode(buf).map(RespFrame::Set),
            None => Err(RespError::NotComplete),
            _ => Err(RespError::InvalidFrameType(format!(
                "Invalid frame type: {:?}",
                buf
            ))),
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use crate::*;
//     use anyhow::Result;
//     use bytes::{BufMut, BytesMut};
// }
