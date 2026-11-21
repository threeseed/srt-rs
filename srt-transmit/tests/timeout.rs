use std::{
    env,
    path::PathBuf,
    process::Stdio,
    time::{Duration, Instant},
};

use bytes::Bytes;
use compio::{
    buf::BufResult,
    io::{AsyncRead, AsyncWriteExt},
    process::Command,
    time::sleep,
};
use futures::prelude::*;

use srt_compio::SrtSocket;

#[cfg(target_os = "windows")]
const STRANSMIT_NAME: &str = "srt-transmit.exe";
#[cfg(not(target_os = "windows"))]
const STRANSMIT_NAME: &str = "srt-transmit";

fn find_stransmit_rs() -> PathBuf {
    let mut stransmit_rs_path = env::current_exe().unwrap();
    stransmit_rs_path.pop();

    stransmit_rs_path.push(STRANSMIT_NAME);

    if !stransmit_rs_path.exists() {
        stransmit_rs_path.pop();
        stransmit_rs_path.pop();
        stransmit_rs_path.push(STRANSMIT_NAME);
    }

    assert!(
        stransmit_rs_path.exists(),
        "Could not find stransmit at {stransmit_rs_path:?}"
    );

    stransmit_rs_path
}

#[compio::test]
async fn receiver_timeout() {
    let _ = pretty_env_logger::try_init();

    let b = SrtSocket::builder().call("127.0.0.1:1878", None);

    let stranmsit_rs = find_stransmit_rs();
    let mut a = Command::new(&stranmsit_rs)
        .args(["-", "srt://:1878"])
        .stdin(Stdio::piped())
        .unwrap()
        .spawn()
        .unwrap();

    let sender = async move {
        let BufResult(res, _) = a.stdin.as_mut().unwrap().write_all(b"asdf").await;
        res.unwrap();
        sleep(Duration::from_millis(2000)).await;

        a.kill().unwrap();
        a.wait().await.unwrap();
    };

    let recvr = async move {
        let mut b = b.await.unwrap();
        assert_eq!(
            b.try_next().await.unwrap().as_ref().map(|t| &*t.1),
            Some(&b"asdf"[..])
        );
        assert_eq!(b.try_next().await.unwrap(), None);
    };
    futures::join!(sender, recvr);
}

#[compio::test]
async fn sender_timeout() {
    let _ = pretty_env_logger::try_init();

    let b = SrtSocket::builder().listen_on(1879);

    let stranmsit_rs = find_stransmit_rs();
    let mut a = Command::new(&stranmsit_rs)
        .args(["srt://localhost:1879", "-"])
        .stdout(Stdio::piped())
        .unwrap()
        .spawn()
        .unwrap();

    let sender = async move {
        let mut b = b.await.unwrap();

        let mut got_done = false;
        for _ in 0..200 {
            if b.send((Instant::now(), Bytes::from_static(b"asdf\n")))
                .await
                .is_err()
            {
                got_done = true;
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
        assert!(got_done);
    };

    let recvr = async move {
        let mut out = a.stdout.take().unwrap();

        // read 10 lines
        let mut lines = 0;
        let mut buf = Vec::with_capacity(1024);
        while lines < 10 {
            buf.clear();
            let BufResult(res, b) = out.read(buf).await;
            buf = b;
            if res.unwrap() == 0 {
                break;
            }
            lines += buf.iter().filter(|c| **c == b'\n').count();
        }

        a.kill().unwrap();
        a.wait().await.unwrap();
    };
    futures::join!(sender, recvr);
}
