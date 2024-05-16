mod echo;
mod hmap;
mod map;

use crate::{Backend, BulkString, RespArray, RespError, RespFrame, SimpleString};
use enum_dispatch::enum_dispatch;
use lazy_static::lazy_static;
use thiserror::Error;
use tracing::info;

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
    Echo(Echo),
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

#[derive(Debug)]
pub struct Echo {
    message: String,
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
        info!("Command: {:?}", value);
        match &value.0 {
            None => Err(CommandError::InvalidCommand(
                "Invalid command, Command must not be RespNullArray".to_string(),
            )),
            Some(vec) => {
                let mut args = vec.iter();
                match args.next() {
                    Some(RespFrame::BulkString(BulkString(Some(ref command)))) => {
                        match command.as_slice() {
                            b"get" => Ok(Get::try_from(value)?.into()),
                            b"set" => Ok(Set::try_from(value)?.into()),
                            b"hget" => Ok(HGet::try_from(value)?.into()),
                            b"hset" => Ok(HSet::try_from(value)?.into()),
                            b"hgetall" => Ok(HGetAll::try_from(value)?.into()),
                            b"echo" => Ok(Echo::try_from(value)?.into()),
                            _ => Ok(Unrecognized.into()),
                        }
                    }
                    _ => Err(CommandError::InvalidCommand(
                        "Invalid command, command must have a BulkString as the first arg"
                            .to_string(),
                    )),
                }
            }
        }
    }
}

pub fn validate_command(
    args: &RespArray,
    name: &str,
    expected_len: usize,
) -> Result<(), CommandError> {
    match args {
        RespArray(None) => {
            return Err(CommandError::InvalidCommand(
                "Invalid command, Command must not be RespNullArray".to_string(),
            ));
        }
        RespArray(Some(ref args)) => {
            match args[0] {
                RespFrame::BulkString(BulkString(Some(ref command))) => {
                    if command != name.as_bytes() {
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
        }
    }

    Ok(())
}

pub fn extract_args(args: RespArray, start: usize) -> Result<Vec<RespFrame>, CommandError> {
    match args.0 {
        None => Err(CommandError::InvalidCommand(
            "Invalid command, Command must not be RespNullArray".to_string(),
        )),
        Some(args) => Ok(args.into_iter().skip(start).collect()),
    }
}
