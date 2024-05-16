mod array;
mod bool;
mod bulk_string;
mod double;
mod frame;
mod integer;
mod map;
mod null;
mod set;
mod simple_error;
mod simple_string;

use bytes::{Buf, BytesMut};
use enum_dispatch::enum_dispatch;
use thiserror::Error;

pub use self::{
    array::RespArray, bulk_string::BulkString, frame::RespFrame, map::RespMap, null::RespNull,
    set::RespSet, simple_error::SimpleError, simple_string::SimpleString,
};

pub const BUF_CAPACITY: usize = 4096;
pub const CRLF_LEN: usize = 2;

#[enum_dispatch]
pub trait RespEncode {
    fn encode(self) -> Vec<u8>;
}

pub trait RespDecode: Sized {
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError>;
}

#[derive(Error, Debug, PartialEq)]
pub enum RespError {
    #[error("Invalid frame: {0}")]
    InvalidFrame(String),
    #[error("Invalid frame type: {0}")]
    InvalidFrameType(String),
    #[error("Invalid frame length: {0}")]
    InvalidFrameLength(isize),
    #[error("Frame is not complete")]
    NotComplete,

    #[error("Parse int error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),
    #[error("Parse float error: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),
}

pub fn extract_fixed_data(buf: &mut BytesMut, expect: &str) -> Result<(), RespError> {
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

pub fn extract_simple_frame_data(
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

pub fn parse_length(buf: &BytesMut, prefix: &str) -> Result<(usize, isize), RespError> {
    let end = extract_simple_frame_data(buf, prefix, 1)?;
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
