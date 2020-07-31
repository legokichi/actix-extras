use super::{DeserializeError, RedisCommand};
use crate::Error;

use actix::Message;
use redis_async::{resp::RespValue, resp_array};

#[derive(Debug)]
pub struct ClusterGetKeysInSlot {
    pub slot: u16,
    pub count: i64,
}

pub fn cluster_get_keys_in_slot(slot: u16, count: i64) -> ClusterGetKeysInSlot {
    ClusterGetKeysInSlot { slot, count }
}

impl RedisCommand for ClusterGetKeysInSlot {
    type Output = Vec<String>;

    fn serialize(self) -> RespValue {
        resp_array![
            "CLUSTER",
            "GETKEYSINSLOT",
            self.slot.to_string(),
            self.count.to_string()
        ]
    }

    fn deserialize(resp: RespValue) -> Result<Self::Output, DeserializeError> {
        use RespValue::*;

        // FromResp returns redis_async::Error, so we need our own version of conversions here
        fn parse_string(resp: RespValue) -> Result<String, DeserializeError> {
            match resp {
                SimpleString(s) => Ok(s),
                BulkString(s) => Ok(String::from_utf8_lossy(&s).into()),
                resp => Err(DeserializeError::new(
                    "CLUSTER GETKEYSINSLOT: not a string",
                    resp,
                )),
            }
        }

        match resp {
            RespValue::Array(v) => v.into_iter().map(parse_string).collect(),
            resp => Err(DeserializeError::new(
                "invalid response to CLUSTER GETKEYSINSLOT",
                resp,
            )),
        }
    }
}

impl Message for ClusterGetKeysInSlot {
    type Result = Result<Vec<String>, Error>;
}
