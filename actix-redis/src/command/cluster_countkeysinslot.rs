use super::{DeserializeError, RedisCommand};
use crate::Error;

use actix::Message;
use redis_async::{resp::RespValue, resp_array};

#[derive(Debug)]
pub struct ClusterCountKeysInSlot {
    pub slot: u16,
}

pub fn cluster_count_keys_in_slot(slot: u16) -> ClusterCountKeysInSlot {
    ClusterCountKeysInSlot { slot }
}

impl RedisCommand for ClusterCountKeysInSlot {
    type Output = i64;

    fn serialize(self) -> RespValue {
        resp_array!["CLUSTER", "COUNTKEYSINSLOT", self.slot.to_string()]
    }

    fn deserialize(resp: RespValue) -> Result<Self::Output, DeserializeError> {
        match resp {
            RespValue::Integer(v) => Ok(v),
            resp => Err(DeserializeError::new(
                "invalid response to CLUSTER COUNTKEYSINSLOT",
                resp,
            )),
        }
    }
}

impl Message for ClusterCountKeysInSlot {
    type Result = Result<i64, Error>;
}
