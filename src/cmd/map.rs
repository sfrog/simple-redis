use super::{extract_args, validate_command, CommandError, CommandExecutor, RESP_OK};
use crate::{Backend, BulkString, RespArray, RespFrame, RespNull};

#[derive(Debug)]
pub struct Get {
    key: String,
}

#[derive(Debug)]
pub struct Set {
    key: String,
    value: RespFrame,
}

impl CommandExecutor for Get {
    fn execute(self, backend: &Backend) -> RespFrame {
        match backend.get(&self.key) {
            Some(value) => value,
            None => RespFrame::Null(RespNull),
        }
    }
}

impl CommandExecutor for Set {
    fn execute(self, backend: &Backend) -> RespFrame {
        backend.set(self.key, self.value);
        RESP_OK.clone()
    }
}

impl TryFrom<RespArray> for Get {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, "get", 1)?;

        let mut args = extract_args(value, 1)?.into_iter();

        match args.next() {
            Some(RespFrame::BulkString(BulkString(Some(key)))) => Ok(Get {
                key: String::from_utf8(key)?,
            }),
            _ => Err(CommandError::InvalidArgument("Invalid key".to_string())),
        }
    }
}

impl TryFrom<RespArray> for Set {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, "set", 2)?;

        let mut args = extract_args(value, 1)?.into_iter();

        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(BulkString(Some(key)))), Some(value)) => Ok(Set {
                key: String::from_utf8(key)?,
                value,
            }),
            _ => Err(CommandError::InvalidArgument(
                "Invalid key or value".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BulkString, RespFrame};
    use anyhow::Result;

    #[test]
    fn test_get_try_from() -> Result<()> {
        let input = RespArray::new(vec![
            RespFrame::BulkString(BulkString::new("get".as_bytes())),
            RespFrame::BulkString(BulkString::new("hello".as_bytes())),
        ]);

        let result = Get::try_from(input)?;

        assert_eq!(result.key, "hello".to_string());

        Ok(())
    }

    #[test]
    fn test_set_try_from() -> Result<()> {
        let input = RespArray::new(vec![
            RespFrame::BulkString(BulkString::new("set".as_bytes())),
            RespFrame::BulkString(BulkString::new("hello".as_bytes())),
            RespFrame::BulkString(BulkString::new("world".as_bytes())),
        ]);

        let result = Set::try_from(input)?;

        assert_eq!(result.key, "hello".to_string());
        assert_eq!(
            result.value,
            RespFrame::BulkString(BulkString::new("world".as_bytes()))
        );

        Ok(())
    }

    #[test]
    fn test_set_get_command() -> Result<()> {
        let backend = Backend::new();

        let set = Set {
            key: "hello".to_string(),
            value: RespFrame::BulkString(BulkString::new("world".as_bytes())),
        };
        let result = set.execute(&backend);
        assert_eq!(result, RESP_OK.clone());

        let get = Get {
            key: "hello".to_string(),
        };
        let result = get.execute(&backend);
        assert_eq!(
            result,
            RespFrame::BulkString(BulkString::new("world".as_bytes()))
        );

        Ok(())
    }
}
