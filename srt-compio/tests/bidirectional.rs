use srt_compio::{options::*, SrtSocket};

use bytes::Bytes;
use compio::runtime::spawn;
use futures::{future, stream, SinkExt, StreamExt, TryStreamExt};
use std::time::{Duration, Instant};

#[compio::test]
async fn bidirectional() {
    let _ = pretty_env_logger::try_init();

    const ITERS: u32 = 1_000;

    let a = SrtSocket::bind(CallerOptions::new("127.0.0.1:5000", None).unwrap().into());
    let b = SrtSocket::bind(ListenerOptions::new(":5000").unwrap().into());

    let mut join_handles = vec![];
    for fut in [a, b] {
        join_handles.push(spawn(async move {
            let side = fut.await.unwrap();
            let (mut s, mut r) = side.split();

            let read = spawn(async move {
                for i in 0..ITERS {
                    let (_, payload) = r.try_next().await.unwrap().unwrap();

                    assert_eq!(payload, Bytes::from(i.to_string()));
                }
                assert_eq!(r.try_next().await.unwrap(), None);
            });
            let mut counting_stream = stream::iter(0..ITERS)
                .then(|i| async move {
                    compio::time::sleep(Duration::from_millis(1)).await;
                    i
                })
                .map(|i| Ok((Instant::now(), Bytes::from(i.to_string()))))
                .boxed_local();

            s.send_all(&mut counting_stream).await.unwrap();
            s.close().await.unwrap();
            read.await.unwrap();
        }));
    }
    future::join_all(join_handles).await;
}
