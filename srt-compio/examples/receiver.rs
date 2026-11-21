use futures::TryStreamExt;
use srt_compio::SrtSocket;
use std::io::Error;

#[compio::main]
async fn main() -> Result<(), Error> {
    let mut srt_socket = SrtSocket::builder().call("127.0.0.1:3333", None).await?;
    let mut count = 0;

    while let Some((_instant, _bytes)) = srt_socket.try_next().await? {
        count += 1;
        print!("\rReceived {count:?} packets");
    }

    println!("\nConnection closed");

    Ok(())
}
