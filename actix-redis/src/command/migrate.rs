use super::{DeserializeError, RedisCommand};
use crate::Error;

use actix::Message;
use redis_async::{resp::RespValue, resp_array};

#[derive(Debug)]
pub struct Migrate {
    pub host: String,
    pub port: u16,
    pub key: String,
    pub db: i64,
    pub timeout: i64,
}

pub fn migrate(host: String, port: u16, key: String, db: i64, timeout: i64) -> Migrate {
    Migrate {
        host,
        port,
        key,
        db,
        timeout,
    }
}

impl RedisCommand for Migrate {
    /// true if key exists, false if not.
    type Output = bool;

    fn serialize(self) -> RespValue {
        resp_array![
            "MIGRATE",
            self.host,
            self.port.to_string(),
            self.key,
            self.db.to_string(),
            self.timeout.to_string()
        ]
    }

    fn deserialize(resp: RespValue) -> Result<Self::Output, DeserializeError> {
        match resp {
            RespValue::SimpleString(s) if s == "OK" => Ok(true),
            RespValue::SimpleString(s) if s == "NOKEY" => Ok(false),
            resp => Err(DeserializeError::new("Invalid response to MIGRATE", resp)),
        }
    }
}

impl Message for Migrate {
    type Result = Result<bool, Error>;
}
