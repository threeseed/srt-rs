//! A small runtime-agnostic watch channel exposed as a [`Stream`].
//!
//! New receivers (and clones) yield the current value first, then every
//! subsequent update. The stream ends once the sender is dropped and the
//! latest value has been observed.

use std::{
    fmt::{self, Debug, Display, Formatter},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use futures::Stream;

#[derive(Debug)]
struct Shared<T> {
    value: T,
    version: u64,
    closed: bool,
    receivers: usize,
    wakers: Vec<Waker>,
}

pub fn channel<T: 'static + Debug + Default + Clone + Send + Sync + Unpin>(
) -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Mutex::new(Shared {
        value: T::default(),
        version: 0,
        closed: false,
        receivers: 1,
        wakers: Vec::new(),
    }));
    (Sender(shared.clone()), Receiver { shared, seen: None })
}

#[derive(Debug, Eq, PartialEq)]
pub struct SendError<T>(pub T);

impl<T> Display for SendError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "watch channel closed")
    }
}

impl<T: Debug> std::error::Error for SendError<T> {}

#[derive(Debug)]
pub struct Sender<T: 'static + Debug + Default + Clone + Send + Sync + Unpin>(
    Arc<Mutex<Shared<T>>>,
);

impl<T: 'static + Debug + Default + Clone + Send + Sync + Unpin> Sender<T> {
    pub fn send(&self, item: T) -> Result<(), SendError<T>> {
        let wakers = {
            let mut shared = self.0.lock().unwrap();
            if shared.receivers == 0 {
                return Err(SendError(item));
            }
            shared.value = item;
            shared.version += 1;
            std::mem::take(&mut shared.wakers)
        };
        wakers.into_iter().for_each(Waker::wake);
        Ok(())
    }
}

impl<T: 'static + Debug + Default + Clone + Send + Sync + Unpin> Drop for Sender<T> {
    fn drop(&mut self) {
        let wakers = {
            let mut shared = self.0.lock().unwrap();
            shared.closed = true;
            std::mem::take(&mut shared.wakers)
        };
        wakers.into_iter().for_each(Waker::wake);
    }
}

#[derive(Debug)]
pub struct Receiver<T: 'static + Debug + Default + Clone + Send + Sync + Unpin> {
    shared: Arc<Mutex<Shared<T>>>,
    seen: Option<u64>,
}

impl<T: 'static + Debug + Default + Clone + Send + Sync + Unpin> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.shared.lock().unwrap().receivers += 1;
        Self {
            shared: self.shared.clone(),
            seen: None,
        }
    }
}

impl<T: 'static + Debug + Default + Clone + Send + Sync + Unpin> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.lock().unwrap().receivers -= 1;
    }
}

impl<T: 'static + Debug + Default + Clone + Send + Sync + Unpin> Stream for Receiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut shared = this.shared.lock().unwrap();
        if this.seen != Some(shared.version) {
            this.seen = Some(shared.version);
            return Poll::Ready(Some(shared.value.clone()));
        }
        if shared.closed {
            return Poll::Ready(None);
        }
        if !shared.wakers.iter().any(|w| w.will_wake(cx.waker())) {
            shared.wakers.push(cx.waker().clone());
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use futures::{executor::block_on, StreamExt};

    use super::*;

    #[test]
    fn yields_current_then_updates_then_ends() {
        let (tx, mut rx) = channel::<u32>();
        assert_eq!(block_on(rx.next()), Some(0));
        tx.send(5).unwrap();
        tx.send(6).unwrap();
        assert_eq!(block_on(rx.next()), Some(6));
        let mut rx2 = rx.clone();
        assert_eq!(block_on(rx2.next()), Some(6));
        drop(tx);
        assert_eq!(block_on(rx.next()), None);
    }

    #[test]
    fn send_fails_without_receivers() {
        let (tx, rx) = channel::<u32>();
        drop(rx);
        assert!(tx.send(1).is_err());
    }
}
