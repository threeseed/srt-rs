use std::{env::args, io::Error, process::exit};

use futures::TryStreamExt;
use srt_compio::{access::*, SrtSocket};

#[compio::main]
async fn main() -> Result<(), Error> {
    if args().len() != 2 {
        eprintln!("Usage: cargo run --example=connect_streamid <username>");
        exit(1);
    }

    let stream_id = format!(
        "{}",
        AccessControlList(vec![StandardAccessControlEntry::UserName(
            args().nth(1).unwrap()
        )
        .into(),])
    );

    let mut srt_socket = SrtSocket::builder()
        .call("127.0.0.1:3333", Some(stream_id.as_str()))
        .await?;

    while let Some((_instant, bytes)) = srt_socket.try_next().await? {
        println!("{}", std::str::from_utf8(&bytes[..]).unwrap());
    }

    println!("Connection closed");

    Ok(())
}
