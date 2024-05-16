use crate::{Backend, RespArray, RespFrame, RespMap, RespNull};

use super::{
    extract_args, validate_command, CommandError, CommandExecutor, HGet, HGetAll, HSet, RESP_OK,
};

impl CommandExecutor for HGet {
    fn execute(self, backend: &Backend) -> RespFrame {
        match backend.hget(&self.key, &self.field) {
            Some(value) => value,
            None => RespFrame::Null(RespNull),
        }
    }
}

impl CommandExecutor for HGetAll {
    fn execute(self, backend: &Backend) -> RespFrame {
        if let Some(map) = backend.hgetall(&self.key) {
            // transform the map into a RespMap
            let mut resp_map = RespMap::new();
            map.into_iter().for_each(|(k, v)| {
                resp_map.insert(k, v);
            });
            resp_map.into()
        } else {
            RespFrame::Null(RespNull)
        }
    }
}

impl CommandExecutor for HSet {
    fn execute(self, backend: &Backend) -> RespFrame {
        backend.hset(self.key, self.field, self.value.clone());
        RESP_OK.clone()
    }
}

impl TryFrom<RespArray> for HGet {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, "hget", 2)?;

        let mut args = extract_args(value, 1).into_iter();

        match (args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(field))) => Ok(HGet {
                key: String::from_utf8(key.0)?,
                field: String::from_utf8(field.0)?,
            }),
            _ => Err(CommandError::InvalidArgument(
                "Invalid key or field".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for HGetAll {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, "hgetall", 1)?;

        let mut args = extract_args(value, 1).into_iter();

        match args.next() {
            Some(RespFrame::BulkString(key)) => Ok(HGetAll {
                key: String::from_utf8(key.0)?,
            }),
            _ => Err(CommandError::InvalidArgument("Invalid key".to_string())),
        }
    }
}

impl TryFrom<RespArray> for HSet {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, "hset", 3)?;

        let mut args = extract_args(value, 1).into_iter();

        match (args.next(), args.next(), args.next()) {
            (Some(RespFrame::BulkString(key)), Some(RespFrame::BulkString(field)), Some(value)) => {
                Ok(HSet {
                    key: String::from_utf8(key.0)?,
                    field: String::from_utf8(field.0)?,
                    value,
                })
            }
            _ => Err(CommandError::InvalidArgument(
                "Invalid key, field or value".to_string(),
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
    fn test_hget_try_from() -> Result<()> {
        let input = RespArray::new(vec![
            RespFrame::BulkString(BulkString::new("hget".as_bytes())),
            RespFrame::BulkString(BulkString::new("map".as_bytes())),
            RespFrame::BulkString(BulkString::new("hello".as_bytes())),
        ]);

        let result = HGet::try_from(input)?;

        assert_eq!(result.key, "map".to_string());
        assert_eq!(result.field, "hello".to_string());

        Ok(())
    }

    #[test]
    fn test_hgetall_try_from() -> Result<()> {
        let input = RespArray::new(vec![
            RespFrame::BulkString(BulkString::new("hgetall".as_bytes())),
            RespFrame::BulkString(BulkString::new("map".as_bytes())),
        ]);

        let result = HGetAll::try_from(input)?;

        assert_eq!(result.key, "map".to_string());

        Ok(())
    }

    #[test]
    fn test_hset_try_from() -> Result<()> {
        let input = RespArray::new(vec![
            RespFrame::BulkString(BulkString::new("hset".as_bytes())),
            RespFrame::BulkString(BulkString::new("map".as_bytes())),
            RespFrame::BulkString(BulkString::new("hello".as_bytes())),
            RespFrame::BulkString(BulkString::new("world".as_bytes())),
        ]);

        let result = HSet::try_from(input)?;

        assert_eq!(result.key, "map".to_string());
        assert_eq!(result.field, "hello".to_string());
        assert_eq!(
            result.value,
            RespFrame::BulkString(BulkString::new("world".as_bytes()))
        );

        Ok(())
    }

    #[test]
    fn test_hget_hset_hgetall_command() -> Result<()> {
        let backend = Backend::new();

        let hset = HSet {
            key: "map".to_string(),
            field: "hello".to_string(),
            value: RespFrame::BulkString(BulkString::new("world".as_bytes())),
        };
        let result = hset.execute(&backend);
        assert_eq!(result, RESP_OK.clone());

        let hget = HGet {
            key: "map".to_string(),
            field: "hello".to_string(),
        };
        let result = hget.execute(&backend);
        assert_eq!(
            result,
            RespFrame::BulkString(BulkString::new("world".as_bytes()))
        );

        let hgetall = HGetAll {
            key: "map".to_string(),
        };
        let result = hgetall.execute(&backend);
        let mut map = RespMap::new();
        map.insert(
            "hello".to_string(),
            RespFrame::BulkString(BulkString::new("world".as_bytes())),
        );
        assert_eq!(result, map.into());

        Ok(())
    }
}
