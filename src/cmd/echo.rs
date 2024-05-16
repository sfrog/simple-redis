use super::{extract_args, validate_command, CommandError, CommandExecutor};
use crate::{Backend, BulkString, RespArray, RespFrame};

#[derive(Debug)]
pub struct Echo {
    message: String,
}

impl CommandExecutor for Echo {
    fn execute(self, _backend: &Backend) -> RespFrame {
        BulkString::new(self.message.as_bytes()).into()
    }
}

impl TryFrom<RespArray> for Echo {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, "echo", 1)?;

        let mut args = extract_args(value, 1)?.into_iter();

        match args.next() {
            Some(RespFrame::BulkString(BulkString(Some(message)))) => Ok(Echo {
                message: String::from_utf8(message)?,
            }),
            _ => Err(CommandError::InvalidArgument("Invalid key".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BulkString, RespFrame};
    use anyhow::Result;

    #[test]
    fn test_echo_try_from() -> Result<()> {
        let input = RespArray::new(vec![
            RespFrame::BulkString(BulkString::new("echo".as_bytes())),
            RespFrame::BulkString(BulkString::new("hello".as_bytes())),
        ]);

        let result = Echo::try_from(input)?;

        assert_eq!(result.message, "hello".to_string());

        Ok(())
    }

    #[test]
    fn test_echo_command() -> Result<()> {
        let backend = Backend::new();

        let set = Echo {
            message: "hello".to_string(),
        };
        let result = set.execute(&backend);
        assert_eq!(result, BulkString::new("hello".as_bytes()).into());

        Ok(())
    }
}
