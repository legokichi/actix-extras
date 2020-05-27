use actix_redis::{command::*, RedisClusterActor};
use std::fmt::Debug;
use std::time::Duration;
use tokio::time::delay_for;

fn success<T: Debug, E1: Debug, E2: Debug>(res: Result<Result<T, E1>, E2>) -> T {
    match res {
        Ok(Ok(x)) => x,
        _ => panic!("Should not happen {:?}", res),
    }
}

#[actix_rt::test]
async fn test_cluster_scaledown() {
    env_logger::init();

    let addr = RedisClusterActor::start("127.0.0.1:7000");

    let key = "test-cluster-scaledown";
    let target_slot = actix_redis::slot::slot(key.as_bytes());

    // target_slot must be served at first
    assert!(success(addr.send(set(key, "value")).await));
    assert_eq!(
        success(addr.send(get(key)).await),
        Some(b"value"[..].into())
    );

    // shut down the master node serving the key
    // TODO: restart the node after the test
    assert!(addr
        .send(DirectedTo {
            command: shutdown(),
            slot: target_slot,
        })
        .await
        .unwrap()
        .is_err());

    // wait until the cluster agrees upon the promotion
    // 2 x cluster-node-timeout + 1 sec
    delay_for(Duration::from_secs(3)).await;

    // make sure that the client can access after the promotion
    assert_eq!(
        success(addr.send(get(key)).await),
        Some(b"value"[..].into())
    );
}
