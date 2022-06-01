use actix_redis::{command, Error, RedisActor};

#[actix_web::test]
async fn test_error_connect() {
    let addr = RedisActor::start("localhost:54000");
    let _addr2 = addr.clone();

    let res = addr.send(command::get("test")).await;
    match res {
        Ok(Err(Error::NotConnected)) => (),
        _ => panic!("Should not happen {:?}", res),
    }
}

#[actix_web::test]
async fn test_redis() {
    env_logger::init();

    let addr = RedisActor::start("127.0.0.1:6379");
    let res = addr.send(command::set("test", "value")).await;

    match res {
        Ok(Ok(success)) => {
            assert!(success);

            let res = addr.send(command::get("test")).await;
            match res {
                Ok(Ok(resp)) => {
                    println!("RESP: {:?}", resp);
                    assert_eq!(resp, Some("value".into()));
                }
                _ => panic!("Should not happen {:?}", res),
            }
        }
        _ => panic!("Should not happen {:?}", res),
    }
}
