use super::{
    extract_args, validate_command, validate_dynamic_command, CommandError, CommandExecutor,
    RESP_OK,
};
use crate::{Backend, BulkString, RespArray, RespFrame, RespNull};

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
pub struct HMGet {
    key: String,
    fields: Vec<String>,
}

#[derive(Debug)]
pub struct HGetAll {
    key: String,
}

impl CommandExecutor for HGet {
    fn execute(self, backend: &Backend) -> RespFrame {
        match backend.hget(&self.key, &self.field) {
            Some(value) => value,
            None => RespFrame::Null(RespNull),
        }
    }
}

impl CommandExecutor for HMGet {
    fn execute(self, backend: &Backend) -> RespFrame {
        let mut ret = Vec::with_capacity(self.fields.len());
        for field in &self.fields {
            match backend.hget(&self.key, field) {
                Some(value) => {
                    ret.push(value);
                }
                None => {
                    ret.push(RespFrame::Null(RespNull));
                }
            }
        }
        RespArray::new(ret).into()
    }
}

impl CommandExecutor for HGetAll {
    fn execute(self, backend: &Backend) -> RespFrame {
        if let Some(map) = backend.hgetall(&self.key) {
            // transform the map into a RespMap
            let mut ret = Vec::with_capacity(map.len() * 2);
            map.into_iter().for_each(|(k, v)| {
                ret.push(BulkString::new(k).into());
                ret.push(v)
            });
            RespArray::new(ret).into()
        } else {
            RespArray::new(Vec::new()).into()
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

        let mut args = extract_args(value, 1)?.into_iter();

        match (args.next(), args.next()) {
            (
                Some(RespFrame::BulkString(BulkString(Some(key)))),
                Some(RespFrame::BulkString(BulkString(Some(field)))),
            ) => Ok(HGet {
                key: String::from_utf8(key)?,
                field: String::from_utf8(field)?,
            }),
            _ => Err(CommandError::InvalidArgument(
                "Invalid key or field".to_string(),
            )),
        }
    }
}

impl TryFrom<RespArray> for HMGet {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_dynamic_command(&value, "hmget", 2)?;

        let mut args = extract_args(value, 1)?.into_iter();

        let key = match args.next() {
            Some(RespFrame::BulkString(BulkString(Some(key)))) => String::from_utf8(key)?,
            _ => return Err(CommandError::InvalidArgument("Invalid key".to_string())),
        };

        let mut fields = Vec::new();
        loop {
            match args.next() {
                Some(RespFrame::BulkString(BulkString(Some(key)))) => {
                    fields.push(String::from_utf8(key)?)
                }
                None => return Ok(HMGet { key, fields }),
                _ => return Err(CommandError::InvalidArgument("Invalid field".to_string())),
            }
        }
    }
}

impl TryFrom<RespArray> for HGetAll {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, "hgetall", 1)?;

        let mut args = extract_args(value, 1)?.into_iter();

        match args.next() {
            Some(RespFrame::BulkString(BulkString(Some(key)))) => Ok(HGetAll {
                key: String::from_utf8(key)?,
            }),
            _ => Err(CommandError::InvalidArgument("Invalid key".to_string())),
        }
    }
}

impl TryFrom<RespArray> for HSet {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, "hset", 3)?;

        let mut args = extract_args(value, 1)?.into_iter();

        match (args.next(), args.next(), args.next()) {
            (
                Some(RespFrame::BulkString(BulkString(Some(key)))),
                Some(RespFrame::BulkString(BulkString(Some(field)))),
                Some(value),
            ) => Ok(HSet {
                key: String::from_utf8(key)?,
                field: String::from_utf8(field)?,
                value,
            }),
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
    fn test_hmget_try_from() -> Result<()> {
        let input = RespArray::new(vec![
            RespFrame::BulkString(BulkString::new("hmget".as_bytes())),
            RespFrame::BulkString(BulkString::new("map".as_bytes())),
            RespFrame::BulkString(BulkString::new("hello".as_bytes())),
            RespFrame::BulkString(BulkString::new("world".as_bytes())),
        ]);

        let result = HMGet::try_from(input)?;

        assert_eq!(result.key, "map".to_string());
        assert_eq!(
            result.fields,
            vec!["hello".to_string(), "world".to_string()]
        );

        Ok(())
    }

    #[test]
    fn test_hget_hset_hgetall_hmget_command() -> Result<()> {
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

        let hset = HSet {
            key: "map".to_string(),
            field: "hello1".to_string(),
            value: RespFrame::BulkString(BulkString::new("world1".as_bytes())),
        };
        let result = hset.execute(&backend);
        assert_eq!(result, RESP_OK.clone());
        let hgetall = HGetAll {
            key: "map".to_string(),
        };
        let result = hgetall.execute(&backend);
        let expected = RespArray::new(vec![
            BulkString::new("hello".as_bytes()).into(),
            BulkString::new("world".as_bytes()).into(),
            BulkString::new("hello1".as_bytes()).into(),
            BulkString::new("world1".as_bytes()).into(),
        ]);
        let expected1 = RespArray::new(vec![
            BulkString::new("hello1".as_bytes()).into(),
            BulkString::new("world1".as_bytes()).into(),
            BulkString::new("hello".as_bytes()).into(),
            BulkString::new("world".as_bytes()).into(),
        ]);
        assert!(result == expected.into() || result == expected1.into());

        let hmget = HMGet {
            key: "map".to_string(),
            fields: vec![
                "hello".to_string(),
                "hello1".to_string(),
                "world".to_string(),
            ],
        };

        let result = hmget.execute(&backend);
        let expected = RespArray::new(vec![
            BulkString::new("world".as_bytes()).into(),
            BulkString::new("world1".as_bytes()).into(),
            RespNull.into(),
        ]);
        assert_eq!(result, expected.into());

        Ok(())
    }
}
