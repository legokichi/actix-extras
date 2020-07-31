//! Redis command types.

mod asking;
mod cluster_countkeysinslot;
mod cluster_getkeysinslot;
pub mod cluster_setslot;
mod cluster_slots;
mod del;
mod echo;
mod get;
mod migrate;
mod ping;
mod set;
mod shutdown;

pub use asking::{asking, Asking};
pub use cluster_countkeysinslot::{cluster_count_keys_in_slot, ClusterCountKeysInSlot};
pub use cluster_getkeysinslot::{cluster_get_keys_in_slot, ClusterGetKeysInSlot};
pub use cluster_setslot::ClusterSetSlot;
pub use cluster_slots::{cluster_slots, ClusterSlots};
pub use del::{del, del_multiple, Del};
pub use echo::{echo, Echo};
pub use get::{get, Get};
pub use migrate::{migrate, Migrate};
pub use ping::{ping, ping_message, Ping};
pub use set::{set, Set};
pub use shutdown::{shutdown, Shutdown};

use redis_async::resp::RespValue;

/// The error type returned when deserializing a response from Redis faild.
#[derive(Clone, Debug)]
pub struct DeserializeError {
    /// Error message.
    pub message: String,
    /// The RESP value (optional).
    pub resp: Option<RespValue>,
}

impl DeserializeError {
    pub fn new<S: Into<String>>(message: S, resp: RespValue) -> Self {
        DeserializeError {
            message: message.into(),
            resp: Some(resp),
        }
    }

    pub fn message<S: Into<String>>(message: S) -> Self {
        DeserializeError {
            message: message.into(),
            resp: None,
        }
    }
}

/// A Redis command.
///
/// Each command type `T` should implement `Message<Result = Result<T::Output,
/// actix_redis::Error>>` so that `RedisActor` can handle the command.
pub trait RedisCommand {
    /// The Rust type of the output of this command.
    type Output;

    /// Serialize the request into `RespValue`.
    fn serialize(self) -> RespValue;
    /// Deserialize the response from `RespValue`.
    fn deserialize(resp: RespValue) -> Result<Self::Output, DeserializeError>;
}

/// A Redis Cluster command.
pub trait RedisClusterCommand: RedisCommand {
    /// Returns a single slot of the keys.
    ///
    /// The command will be sent to a node according to the slot.
    ///
    /// # Errors
    ///
    /// This method will return an error if the keys have different slots,
    /// as such a request may be rejected by Redis if the slots are served by different nodes.
    fn slot(&self) -> Result<u16, Vec<u16>>;
}

/// A Redis command directed to a node serving the slot
#[derive(Debug)]
pub struct DirectedTo<C> {
    pub command: C,
    pub slot: u16,
}

impl<C: RedisCommand> RedisCommand for DirectedTo<C> {
    type Output = C::Output;

    fn serialize(self) -> RespValue {
        self.command.serialize()
    }

    fn deserialize(resp: RespValue) -> Result<Self::Output, DeserializeError> {
        C::deserialize(resp)
    }
}

impl<C: RedisCommand> RedisClusterCommand for DirectedTo<C> {
    fn slot(&self) -> Result<u16, Vec<u16>> {
        Ok(self.slot)
    }
}

impl<C: actix::Message> actix::Message for DirectedTo<C> {
    type Result = C::Result;
}
