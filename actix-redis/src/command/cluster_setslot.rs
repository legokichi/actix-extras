use super::{DeserializeError, RedisCommand};
use crate::Error;

use actix::Message;
use redis_async::{resp::RespValue, resp_array};

#[derive(Debug)]
pub enum ClusterSetSlot {
    Importing { slot: u16, source_node_id: String },
    Migrating { slot: u16, dest_node_id: String },
    Stable { slot: u16 },
    Node { slot: u16, node_id: String },
}

pub fn importing(slot: u16, source_node_id: String) -> ClusterSetSlot {
    ClusterSetSlot::Importing {
        slot,
        source_node_id,
    }
}

pub fn migrating(slot: u16, dest_node_id: String) -> ClusterSetSlot {
    ClusterSetSlot::Migrating { slot, dest_node_id }
}

pub fn stable(slot: u16) -> ClusterSetSlot {
    ClusterSetSlot::Stable { slot }
}

pub fn node(slot: u16, node_id: String) -> ClusterSetSlot {
    ClusterSetSlot::Node { slot, node_id }
}

impl RedisCommand for ClusterSetSlot {
    type Output = ();

    fn serialize(self) -> RespValue {
        use ClusterSetSlot::*;

        match self {
            Importing {
                slot,
                source_node_id,
            } => resp_array![
                "CLUSTER",
                "SETSLOT",
                slot.to_string(),
                "IMPORTING",
                source_node_id
            ],
            Migrating { slot, dest_node_id } => resp_array![
                "CLUSTER",
                "SETSLOT",
                slot.to_string(),
                "MIGRATING",
                dest_node_id
            ],
            Stable { slot } => {
                resp_array!["CLUSTER", "SETSLOT", slot.to_string(), "STABLE"]
            }
            Node { slot, node_id } => {
                resp_array!["CLUSTER", "SETSLOT", slot.to_string(), "NODE", node_id]
            }
        }
    }

    fn deserialize(resp: RespValue) -> Result<Self::Output, DeserializeError> {
        match resp {
            RespValue::SimpleString(s) if s == "OK" => Ok(()),
            resp => Err(DeserializeError::new(
                "invalid response to CLUSTER SETSLOT",
                resp,
            )),
        }
    }
}

impl Message for ClusterSetSlot {
    type Result = Result<(), Error>;
}
