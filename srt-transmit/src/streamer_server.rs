use std::{
    cell::RefCell,
    io,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
    time::Instant,
};

use bytes::Bytes;
use compio::runtime::spawn;
use futures::{
    channel::{mpsc, oneshot},
    select,
    sink::Sink,
    FutureExt, SinkExt, StreamExt,
};
use log::warn;

use srt_compio::{
    options::{ListenerOptions, Valid},
    SrtIncoming, SrtListener, SrtSocket,
};

// Per-subscriber queue depth. When a subscriber falls this far behind, packets are
// dropped for it (and the count logged), similar to a lagging broadcast receiver.
const SUBSCRIBER_CAPACITY: usize = 10_000;

type Item = (Instant, Bytes);

struct Subscriber {
    sender: mpsc::Sender<Item>,
    dropped: u64,
}

type Subscribers = Rc<RefCell<Vec<Subscriber>>>;

// Even though it is never directly used, oneshot::Sender<()> is not dead code. It is used to
// ensures the run_receive_loop task terminates when the StreamerServer is dropped
#[allow(dead_code)]
pub struct StreamerServer(Subscribers, oneshot::Sender<()>);

impl StreamerServer {
    pub async fn bind(options: Valid<ListenerOptions>) -> Result<Self, io::Error> {
        let subscribers: Subscribers = Default::default();
        let (cancel_sender, cancel_receiver) = oneshot::channel();

        let (listener, incoming) = SrtListener::bind(options).await?;
        spawn(Self::run_receive_loop(
            listener,
            incoming,
            cancel_receiver,
            subscribers.clone(),
        ))
        .detach();

        Ok(StreamerServer(subscribers, cancel_sender))
    }

    async fn run_receive_loop(
        _listener: SrtListener,
        mut incoming: SrtIncoming,
        cancel: oneshot::Receiver<()>,
        subscribers: Subscribers,
    ) {
        let mut incoming = incoming.incoming().fuse();
        let mut cancel = cancel.fuse();
        while let Some(request) = select!(
                    _ = cancel => return,
                    result = incoming.next() => result)
        {
            let sender = request.accept(None).await.unwrap();
            let (tx, rx) = mpsc::channel(SUBSCRIBER_CAPACITY);
            subscribers.borrow_mut().push(Subscriber {
                sender: tx,
                dropped: 0,
            });
            spawn(Self::run_send_loop(sender, rx)).detach();
        }
    }

    async fn run_send_loop(mut sender: SrtSocket, mut input: mpsc::Receiver<Item>) {
        while let Some(data) = input.next().await {
            if sender.send(data).await.is_err() {
                break;
            }
        }
        let _ = sender.close().await;
    }
}

impl Sink<Item> for StreamerServer {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Item) -> Result<(), Self::Error> {
        self.0
            .borrow_mut()
            .retain_mut(|sub| match sub.sender.try_send(item.clone()) {
                Ok(()) => {
                    if sub.dropped > 0 {
                        warn!("Stream server dropped packets {}", sub.dropped);
                        sub.dropped = 0;
                    }
                    true
                }
                Err(e) if e.is_full() => {
                    sub.dropped += 1;
                    true
                }
                // the send loop for this subscriber has ended
                Err(_) => false,
            });
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        // closing the channels lets each send loop drain its queue and close its socket
        for sub in self.0.borrow_mut().iter_mut() {
            sub.sender.close_channel();
        }
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::SrtSocket;
    use anyhow::Result;
    use bytes::Bytes;
    use compio::{runtime::spawn, time::sleep};
    use futures::future::join_all;
    use log::info;

    use super::*;

    #[compio::test]
    async fn multiplexer() -> Result<()> {
        let _ = pretty_env_logger::try_init();

        let listener = spawn(async {
            let options = ListenerOptions::new(2000).unwrap();
            let mut server = StreamerServer::bind(options).await.unwrap();
            let end = Instant::now() + Duration::from_secs(1);
            let mut count = 0;
            while end > Instant::now() {
                count += 1;
                server
                    .send((Instant::now(), Bytes::from(format!("asdf {count}"))))
                    .await
                    .unwrap();
                sleep(Duration::from_millis(10)).await;
            }
            server.close().await.unwrap();
            info!("Sender finished");
        });

        // connect 10 clients to it
        let mut join_handles = vec![];
        for i in 0..3 {
            join_handles.push(spawn(async move {
                let mut recvr = SrtSocket::builder()
                    .call("127.0.0.1:2000", None)
                    .await
                    .unwrap();

                info!("Created connection {}", i);

                while let Some(data) = recvr.next().await {
                    info!("Data {}: {:?}", i, data);
                    assert!(data.unwrap().1.starts_with("asdf".as_bytes()));
                }

                info!("Receiving done {}", i);
            }));
        }

        // close the multiplex server when all is done
        for result in join_all(join_handles).await {
            result.unwrap();
        }
        info!("all finished");
        listener.await.unwrap();
        Ok(())
    }
}
