use actix_redis::{command::*, RedisClusterActor};
use futures::stream::{FuturesUnordered, StreamExt};
use std::convert::TryInto;
use std::fmt::Debug;
use std::time::Duration;
use tokio::time;

fn success<T: Debug, E1: Debug, E2: Debug>(res: Result<Result<T, E1>, E2>) -> T {
    match res {
        Ok(Ok(x)) => x,
        _ => panic!("Should not happen {:?}", res),
    }
}

#[actix_rt::test]
async fn test_cluster_redirect() {
    env_logger::init();

    let addr = RedisClusterActor::start("127.0.0.1:7000");

    let set = set("test-moved", "value");
    let slot = set.slot().unwrap();

    success(addr.send(set).await);
    let slots = success(
        addr.send(DirectedTo {
            command: cluster_slots(),
            slot: 0,
        })
        .await,
    );

    let mut source = None;
    let mut source_slot = None;
    let mut destination = None;
    let mut destination_slot = None;

    // find a slot where `test-moved` is stored and another slot (where `test-moved` is NOT stored)
    for slots in slots.into_iter() {
        if slots.start <= slot && slot <= slots.end {
            source = Some(slots.nodes[0].clone());
            source_slot = Some(slots.start);
        } else {
            destination = Some(slots.nodes[0].clone());
            destination_slot = Some(slots.start);
        }

        if source.is_some() && destination.is_some() {
            break;
        }
    }

    let (_source_host, _source_port, source_id) = source.unwrap();
    let source_id = source_id.unwrap();
    let (destination_host, destination_port, destination_id) = destination.unwrap();
    let destination_id = destination_id.unwrap();
    let source_slot = source_slot.unwrap();
    let destination_slot = destination_slot.unwrap();

    success(
        addr.send(DirectedTo {
            command: cluster_setslot::importing(slot, source_id.clone()),
            slot: destination_slot,
        })
        .await,
    );
    success(
        addr.send(DirectedTo {
            command: cluster_setslot::migrating(slot, destination_id.clone()),
            slot: source_slot,
        })
        .await,
    );

    let mut count: usize = success(
        addr.send(DirectedTo {
            command: cluster_count_keys_in_slot(slot),
            slot: source_slot,
        })
        .await,
    )
    .try_into()
    .unwrap();

    while count > 0 {
        let keys = success(
            addr.send(DirectedTo {
                command: cluster_get_keys_in_slot(slot, 10),
                slot: source_slot,
            })
            .await,
        );
        let len = keys.len();

        let migration: FuturesUnordered<_> = keys
            .into_iter()
            .map(|key| {
                addr.send(DirectedTo {
                    command: migrate(destination_host.clone(), destination_port, key, 0, 100),
                    slot: source_slot,
                })
            })
            .collect();

        let res: Vec<_> = migration.collect().await;
        for res in res.into_iter() {
            assert!(success(res));
        }

        count -= len;
    }

    // test ASK redirection
    assert_eq!(
        success(addr.send(get("test-moved")).await).unwrap(),
        b"value"
    );

    success(
        addr.send(DirectedTo {
            command: cluster_setslot::node(slot, destination_id),
            slot: destination_slot,
        })
        .await,
    );

    // wait until migration completes
    time::sleep(Duration::from_secs(3)).await;

    // test MOVED redirection
    assert_eq!(
        success(addr.send(get("test-moved")).await).unwrap(),
        b"value"
    );
}
