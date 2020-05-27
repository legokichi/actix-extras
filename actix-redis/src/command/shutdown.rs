use super::{DeserializeError, RedisCommand};
use crate::Error;

use actix::Message;
use redis_async::resp::RespValue;

/// SHUTDOWN command.
#[derive(Debug)]
pub struct Shutdown {
    no_save: bool,
    save: bool,
}

impl Shutdown {
    /// Force Redis to not save data.
    pub fn no_save(self) -> Self {
        Self {
            no_save: true,
            ..self
        }
    }

    /// Force Redis to save data.
    pub fn save(self) -> Self {
        Self { save: true, ..self }
    }
}

/// SHUTDOWN command.
pub fn shutdown() -> Shutdown {
    Shutdown {
        no_save: false,
        save: false,
    }
}

impl RedisCommand for Shutdown {
    /// Simple string reply *on error*.
    /// If the command succeeds, connection is closed.
    type Output = String;

    fn serialize(self) -> RespValue {
        let mut payload = vec!["SHUTDOWN".into()];

        if self.save {
            payload.push("SAVE".into());
        }

        if self.no_save {
            payload.push("NOSAVE".into());
        }

        RespValue::Array(payload)
    }

    fn deserialize(resp: RespValue) -> Result<Self::Output, DeserializeError> {
        match resp {
            RespValue::SimpleString(s) => Ok(s),
            resp => Err(DeserializeError::new("Invalid response to SHUTDOWN", resp)),
        }
    }
}

impl Message for Shutdown {
    type Result = Result<<Shutdown as RedisCommand>::Output, Error>;
}
