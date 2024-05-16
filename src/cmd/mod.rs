mod hmap;
mod map;

use crate::{Backend, RespArray, RespError, RespFrame, SimpleString};
use enum_dispatch::enum_dispatch;
use lazy_static::lazy_static;
use thiserror::Error;

lazy_static! {
    static ref RESP_OK: RespFrame = SimpleString::new("OK").into();
}

#[derive(Error, Debug)]
pub enum CommandError {
    #[error("Invalid command: {0}")]
    InvalidCommand(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("{0}")]
    RespError(#[from] RespError),
    #[error("{0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),
}

#[enum_dispatch]
pub trait CommandExecutor {
    fn execute(self, backend: &Backend) -> RespFrame;
}

#[enum_dispatch(CommandExecutor)]
#[derive(Debug)]
pub enum Command {
    Get(Get),
    Set(Set),
    HGet(HGet),
    HSet(HSet),
    HGetAll(HGetAll),
    Unrecognized(Unrecognized),
}

impl CommandExecutor for Unrecognized {
    fn execute(self, _: &Backend) -> RespFrame {
        RESP_OK.clone()
    }
}

#[derive(Debug)]
pub struct Unrecognized;

#[derive(Debug)]
pub struct Get {
    key: String,
}

#[derive(Debug)]
pub struct Set {
    key: String,
    value: RespFrame,
}

#[derive(Debug)]
pub struct HGet {
    key: String,
    field: String,
}

#[derive(Debug)]
pub struct HSet {
    key: String,
    field: String,
    value: RespFrame,
}

#[derive(Debug)]
pub struct HGetAll {
    key: String,
}

impl TryFrom<RespFrame> for Command {
    type Error = CommandError;

    fn try_from(value: RespFrame) -> Result<Self, Self::Error> {
        match value {
            RespFrame::Array(value) => Command::try_from(value),
            _ => Err(CommandError::InvalidCommand(
                "Invalid command, Command must be RespArray".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for Command {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        let mut args = value.clone().0.into_iter();

        match args.next() {
            Some(RespFrame::BulkString(ref command)) => match command.0.as_slice() {
                b"get" => Ok(Command::Get(Get::try_from(value)?)),
                b"set" => Ok(Command::Set(Set::try_from(value)?)),
                b"hget" => Ok(Command::HGet(HGet::try_from(value)?)),
                b"hset" => Ok(Command::HSet(HSet::try_from(value)?)),
                b"hgetall" => Ok(Command::HGetAll(HGetAll::try_from(value)?)),
                _ => Ok(Command::Unrecognized(Unrecognized)),
            },
            _ => Err(CommandError::InvalidCommand(
                "Invalid command, command must have a BulkString as the first arg".to_string(),
            )),
        }
    }
}

pub fn validate_command(
    args: &RespArray,
    name: &str,
    expected_len: usize,
) -> Result<(), CommandError> {
    match args[0] {
        RespFrame::BulkString(ref command) => {
            if command.0 != name.as_bytes() {
                return Err(CommandError::InvalidCommand(format!(
                    "Invalid command: expected {}",
                    name
                )));
            }
        }
        _ => {
            return Err(CommandError::InvalidCommand(format!(
                "Invalid command: expected {}",
                name
            )))
        }
    }

    if args.len() != expected_len + 1 {
        return Err(CommandError::InvalidArgument(format!(
            "{} command must have exactly {} arguments",
            name, expected_len
        )));
    }

    Ok(())
}

pub fn extract_args(args: RespArray, start: usize) -> Vec<RespFrame> {
    args.0.into_iter().skip(start).collect()
}
