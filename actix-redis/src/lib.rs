//! Redis integration for `actix`.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms, nonstandard_style)]
#![warn(future_incompatible)]

mod cluster;
pub mod command;
mod redis;
pub mod slot;

pub use cluster::RedisClusterActor;
use derive_more::{Display, Error, From};
pub use redis::{Command, RedisActor};

/// General purpose `actix-redis` error.
#[derive(Debug, Display, Error, From)]
pub enum Error {
    #[display(fmt = "Redis error {}", _0)]
    Redis(redis_async::error::Error),
    #[display(fmt = "Redis Cluster: Different slots")]
    DifferentSlots(#[error(not(source))] Vec<u16>),
    /// Receiving message during reconnecting
    #[display(fmt = "Redis: Not connected")]
    NotConnected,
    /// Cancel all waters when connection get dropped
    #[display(fmt = "Redis: Disconnected")]
    Disconnected,
}

#[cfg(feature = "web")]
impl actix_web::ResponseError for Error {}

/// The range of the slots served by a node
#[derive(Clone, Debug)]
pub struct Slots {
    pub start: u16,
    pub end: u16,
    /// IP address, port, id of nodes serving the slots.
    /// The first entry corresponds to the master node.
    pub nodes: Vec<(String, u16, Option<String>)>,
}

impl Slots {
    // Address of the master node in `addr:port` format.
    fn master_addr(&self) -> String {
        format!("{}:{}", self.nodes[0].0, self.nodes[0].1)
    }
}

// re-export
pub use redis_async::error::Error as RespError;
pub use redis_async::resp::RespValue;
pub use redis_async::resp_array;
