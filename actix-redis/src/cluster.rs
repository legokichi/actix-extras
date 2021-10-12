use actix::prelude::*;
use actix_utils::oneshot;
use futures_util::future::FutureExt;
use log::{debug, info, warn};
use redis_async::resp::RespValue;

use std::collections::HashMap;

use crate::command::{Asking, ClusterSlots, RedisClusterCommand, RedisCommand};
use crate::{Error, RedisActor, RespError, Slots};

const MAX_RETRY: usize = 16;

// Formats RESP value in UTF-8 (lossy).
struct DebugResp<'a>(&'a RespValue);

impl std::fmt::Debug for DebugResp<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            RespValue::Nil => write!(f, "nil"),
            RespValue::Integer(n) => write!(f, "{:?}", n),
            RespValue::Array(o) => f.debug_list().entries(o.iter().map(DebugResp)).finish(),
            RespValue::SimpleString(s) => write!(f, "{:?}", s),
            RespValue::BulkString(s) => write!(f, "{:?}", String::from_utf8_lossy(s)),
            RespValue::Error(e) => write!(f, "{:?}", e),
        }
    }
}

pub struct RedisClusterActor {
    initial_addr: String,
    slots: Vec<Slots>,
    connections: HashMap<String, Addr<RedisActor>>,
}

impl RedisClusterActor {
    pub fn start<S: Into<String>>(addr: S) -> Addr<RedisClusterActor> {
        let addr = addr.into();

        Supervisor::start(move |_ctx| RedisClusterActor {
            initial_addr: addr,
            slots: vec![],
            connections: HashMap::new(),
        })
    }

    fn refresh_slots(&mut self) -> ResponseActFuture<Self, ()> {
        let addr = self.initial_addr.clone();
        let control_connection = self
            .connections
            .entry(addr.clone())
            .or_insert_with(move || RedisActor::start(addr));

        Box::pin(
            control_connection
                .send(ClusterSlots)
                .map(|res| match res {
                    Ok(Ok(slots)) => Ok(slots),
                    Ok(Err(e)) => Err(e),
                    Err(_) => Err(Error::Disconnected),
                })
                .into_actor(self)
                .map(|res, this, _ctx| match res {
                    Ok(slots) => {
                        for slots in slots.iter() {
                            this.connections
                                .entry(slots.master_addr().to_string())
                                .or_insert_with(|| RedisActor::start(slots.master_addr()));
                        }
                        this.slots = slots;
                        debug!("slots: {:?}", this.slots);
                    }
                    Err(e) => {
                        warn!("refreshing slots failed: {:?}", e);
                    }
                }),
        )
    }

    fn dispatch(
        &mut self,
        slot: u16,
        addr: Option<String>,
        req: RespValue,
        retry: usize,
        sender: oneshot::Sender<Result<RespValue, Error>>,
    ) -> ResponseActFuture<Self, ()> {
        debug!(
            "processing: slot = {}, addr = {:?}, retry = {}, request = {:?}",
            slot,
            addr,
            retry,
            DebugResp(&req)
        );

        // If the node address is specified (in the case of redirection), use the address.
        // Otherwise, select the node based on slot information.
        let addr = match addr {
            Some(addr) => addr,
            None => {
                if let Some(slots) = self
                    .slots
                    .iter()
                    .find(|slots| slots.start <= slot && slot <= slots.end)
                {
                    slots.master_addr()
                } else {
                    warn!("no node is serving the slot {}", slot);
                    let _ = sender.send(Err(Error::NotConnected));
                    return Box::pin(actix::fut::ready(()));
                }
            }
        };

        let connection = self
            .connections
            .entry(addr.clone())
            .or_insert_with(move || RedisActor::start(addr));
        Box::pin(
            connection
                .send(crate::redis::Command(req.clone()))
                .into_actor(self)
                .map(move |res, this, ctx| {
                    debug!(
                        "received: {:?}",
                        res.as_ref().map(|res| res.as_ref().map(DebugResp))
                    );
                    match res {
                        // In the case of MOVED and ASK redirection, a retry is performed without
                        // waiting for the slot information to be updated. This is because the
                        // redirection includes the destination node information.
                        //
                        // TODO: Throttle the slot information update
                        Ok(Ok(RespValue::Error(ref e)))
                            if e.starts_with("MOVED") && retry < MAX_RETRY =>
                        {
                            info!(
                                "MOVED redirection: retry = {}, request = {:?}",
                                retry,
                                DebugResp(&req)
                            );

                            let mut values = e.split(' ');
                            let _moved = values.next().unwrap();
                            let _slot = values.next().unwrap();
                            let addr = values.next().unwrap().to_string();

                            ctx.spawn(this.dispatch(slot, Some(addr), req, retry + 1, sender));
                            ctx.spawn(this.refresh_slots());
                        }
                        Ok(Ok(RespValue::Error(ref e)))
                            if e.starts_with("ASK") && retry < MAX_RETRY =>
                        {
                            info!(
                                "ASK redirection: retry = {}, request = {:?}",
                                retry,
                                DebugResp(&req)
                            );

                            let mut values = e.split(' ');
                            let _moved = values.next().unwrap();
                            let _slot = values.next().unwrap();
                            let addr = values.next().unwrap().to_string();

                            let (asking_sender, asking_receiver) = oneshot::channel();
                            ctx.spawn(
                                // No retry for ASKING
                                this.dispatch(
                                    slot,
                                    Some(addr.clone()),
                                    Asking.serialize(),
                                    MAX_RETRY,
                                    asking_sender,
                                ),
                            );
                            ctx.spawn(asking_receiver.into_actor(this).then(
                                move |res, this, _ctx| {
                                    match res.map(|res| res.map(Asking::deserialize)) {
                                        Ok(Ok(Ok(()))) => {
                                            this.dispatch(slot, Some(addr), req, retry + 1, sender)
                                        }
                                        e => {
                                            warn!("failed to issue ASKING: {:?}", e);

                                            // If the ASKING issue fails, it is likely that the
                                            // Redis cluster is not yet stable. In this case, there
                                            // is nothing to be done but to try again.
                                            this.dispatch(slot, None, req, retry + 1, sender)
                                        }
                                    }
                                },
                            ));
                        }
                        // The client also retries when it loses the connection to the Redis node
                        // (e.g., in the case of resharding). In this case, since the correct
                        // destination is not known, it waits for the slot information to be
                        // updated before redirecting.
                        Ok(Err(Error::NotConnected)) if retry < MAX_RETRY => {
                            warn!("redis node is not connected");
                            this.connections.clear();
                            ctx.wait(this.refresh_slots().map(move |(), this, ctx| {
                                ctx.spawn(this.dispatch(slot, None, req, retry + 1, sender));
                            }));
                        }
                        Ok(Ok(RespValue::Error(ref e)))
                            if e.starts_with("CLUSTERDOWN") && retry < MAX_RETRY =>
                        {
                            warn!("redis cluster is down: {:?}", e);
                            this.connections.clear();
                            ctx.wait(this.refresh_slots().map(move |(), this, ctx| {
                                ctx.spawn(this.dispatch(slot, None, req, retry + 1, sender));
                            }));
                        }
                        // Succeeded
                        Ok(res) => {
                            let _ = sender.send(res);
                        }
                        // Redis Actor is down
                        Err(_canceled) => {
                            let _ = sender.send(Err(Error::Disconnected));
                        }
                    }
                }),
        )
    }
}

impl Actor for RedisClusterActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.wait(self.refresh_slots());
    }
}

impl Supervised for RedisClusterActor {
    fn restarting(&mut self, _: &mut Self::Context) {
        self.slots.clear();
        self.connections.clear();
    }
}

impl<T> Handler<T> for RedisClusterActor
where
    T: RedisClusterCommand + Message<Result = Result<<T as RedisCommand>::Output, Error>>,
    T::Output: Send + 'static,
{
    type Result = ResponseFuture<Result<T::Output, Error>>;

    fn handle(&mut self, msg: T, ctx: &mut Self::Context) -> Self::Result {
        // refuse operations over multiple slots
        let slot = match msg.slot() {
            Ok(slot) => slot,
            Err(e) => return Box::pin(futures_util::future::err(Error::DifferentSlots(e))),
        };
        let req = msg.serialize();

        let (sender, receiver) = oneshot::channel();
        ctx.spawn(self.dispatch(slot, None, req, 0, sender));
        Box::pin(receiver.map(|res| match res {
            Ok(Ok(res)) => {
                T::deserialize(res).map_err(|e| Error::Redis(RespError::RESP(e.message, e.resp)))
            }
            Ok(Err(e)) => Err(e),
            Err(_canceled) => Err(Error::Disconnected),
        }))
    }
}
