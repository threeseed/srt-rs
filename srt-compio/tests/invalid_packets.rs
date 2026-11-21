use bytes::Bytes;
use compio::{net::UdpSocket, time::sleep};
use futures::prelude::*;
use futures::stream;
use log::info;
use rand::{prelude::StdRng, Rng, SeedableRng};
use srt_compio::SrtSocket;
use std::time::{Duration, Instant};

// Send a bunch of invalid packets to the socket, making sure that it can handle it
#[compio::test]
async fn invalid_packets() {
    let _ = pretty_env_logger::try_init();

    let sender = async {
        let mut sender = SrtSocket::builder()
            .local_port(8877)
            .call("127.0.0.1:8876", None)
            .await
            .unwrap();

        let mut counting_stream = stream::iter(0..100)
            .then(|i| async move {
                compio::time::sleep(Duration::from_millis(1)).await;
                i
            })
            .map(|i| Ok((Instant::now(), Bytes::from(i.to_string()))))
            .boxed_local();

        sender.send_all(&mut counting_stream).await.unwrap();
        sender.close().await.unwrap();
    };
    let recvr = async {
        let mut recvr = SrtSocket::builder().listen_on(":8876").await.unwrap();

        info!("Receiver initialised");

        for _ in 0..100 {
            recvr.try_next().await.unwrap().unwrap();
        }

        assert_eq!(recvr.try_next().await.unwrap(), None);
    };

    let garbage = compio::runtime::spawn(async move {
        // seed the rng
        let s = match std::env::var("INVALID_PACKETS_SEED") {
            Ok(s) => {
                info!("Using seed from env");
                s.parse().unwrap()
            }
            Err(_) => rand::random(),
        };
        info!("Seed is {}", s);
        let mut rng = StdRng::seed_from_u64(s);

        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        loop {
            sleep(Duration::from_millis(1)).await;
            let mut to_send = vec![0; rng.gen_range(1..=1024)];
            for i in &mut to_send {
                *i = rng.gen();
            }

            sock.send_to(to_send.clone(), "127.0.0.1:8876")
                .await
                .0
                .unwrap();
            sock.send_to(to_send, "127.0.0.1:8877").await.0.unwrap();
        }
    });

    futures::select! {
        _ = futures::future::join(sender, recvr).fuse() => {},
        _ = garbage.fuse() => {}
    }
}
