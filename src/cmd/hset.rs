use super::{
    extract_args, validate_command, validate_dynamic_command, CommandError, CommandExecutor,
};
use crate::{Backend, BulkString, RespArray, RespFrame};

#[derive(Debug)]
pub struct SAdd {
    key: String,
    members: Vec<String>,
}

#[derive(Debug)]
pub struct SIsMember {
    key: String,
    member: String,
}

impl CommandExecutor for SAdd {
    fn execute(self, backend: &Backend) -> RespFrame {
        let mut added: i64 = 0;
        for member in self.members {
            let ret = backend.sadd(self.key.clone(), member);
            added += ret as i64;
        }
        added.into()
    }
}

impl CommandExecutor for SIsMember {
    fn execute(self, backend: &Backend) -> RespFrame {
        let ret = backend.sismember(&self.key, &self.member);
        let ret = if ret { 1 } else { 0 };
        // ret.into()
        RespFrame::Integer(ret)
    }
}

impl TryFrom<RespArray> for SAdd {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_dynamic_command(&value, "sadd", 2)?;

        let mut args = extract_args(value, 1)?.into_iter();

        let key = match args.next() {
            Some(RespFrame::BulkString(BulkString(Some(key)))) => String::from_utf8(key)?,
            _ => return Err(CommandError::InvalidArgument("Invalid key".to_string())),
        };

        let mut members = Vec::new();
        loop {
            match args.next() {
                Some(RespFrame::BulkString(BulkString(Some(key)))) => {
                    members.push(String::from_utf8(key)?)
                }
                None => return Ok(SAdd { key, members }),
                _ => return Err(CommandError::InvalidArgument("Invalid member".to_string())),
            }
        }
    }
}

impl TryFrom<RespArray> for SIsMember {
    type Error = CommandError;

    fn try_from(value: RespArray) -> Result<Self, Self::Error> {
        validate_command(&value, "sismember", 2)?;

        let mut args = extract_args(value, 1)?.into_iter();

        match (args.next(), args.next()) {
            (
                Some(RespFrame::BulkString(BulkString(Some(key)))),
                Some(RespFrame::BulkString(BulkString(Some(member)))),
            ) => Ok(SIsMember {
                key: String::from_utf8(key)?,
                member: String::from_utf8(member)?,
            }),
            _ => Err(CommandError::InvalidArgument(
                "Invalid key or member".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_try_from_sadd() -> Result<()> {
        let input = RespArray::new(vec![
            RespFrame::BulkString(BulkString(Some("sadd".as_bytes().to_vec()))),
            RespFrame::BulkString(BulkString(Some("key".as_bytes().to_vec()))),
            RespFrame::BulkString(BulkString(Some("member1".as_bytes().to_vec()))),
            RespFrame::BulkString(BulkString(Some("member2".as_bytes().to_vec()))),
        ]);

        let cmd = SAdd::try_from(input)?;
        assert_eq!(cmd.key, "key");
        assert_eq!(cmd.members, vec!["member1", "member2"]);

        Ok(())
    }

    #[test]
    fn test_try_from_sismember() -> Result<()> {
        let input = RespArray::new(vec![
            RespFrame::BulkString(BulkString(Some("sismember".as_bytes().to_vec()))),
            RespFrame::BulkString(BulkString(Some("key".as_bytes().to_vec()))),
            RespFrame::BulkString(BulkString(Some("member".as_bytes().to_vec()))),
        ]);

        let cmd = SIsMember::try_from(input)?;
        assert_eq!(cmd.key, "key");
        assert_eq!(cmd.member, "member");

        Ok(())
    }

    #[test]
    fn test_sadd_sismember_execute() {
        let backend = Backend::new();
        let cmd = SAdd {
            key: "key".to_string(),
            members: vec!["member1".to_string(), "member2".to_string()],
        };

        let ret = cmd.execute(&backend);
        assert_eq!(ret, 2.into());

        let cmd = SAdd {
            key: "key".to_string(),
            members: vec!["member1".to_string(), "member3".to_string()],
        };

        let ret = cmd.execute(&backend);
        assert_eq!(ret, 1.into());

        let cmd = SIsMember {
            key: "key".to_string(),
            member: "member1".to_string(),
        };
        let ret = cmd.execute(&backend);
        assert_eq!(ret, 1.into());

        let cmd = SIsMember {
            key: "key".to_string(),
            member: "member".to_string(),
        };
        let ret = cmd.execute(&backend);
        assert_eq!(ret, 0.into());
    }
}
