//! A single background thread running a compio runtime.
//!
//! compio runtimes are thread-local and single threaded, while the C API may be called from any
//! thread. All work that has to happen on the runtime (socket creation, connecting, listening,
//! timers) is sent to this thread as a closure that builds the future there. Results are handed
//! back through `futures` oneshot channels, which can be awaited from any thread.

use std::{cell::Cell, future::Future, thread, time::Duration};

use futures::{
    channel::{mpsc, oneshot},
    StreamExt,
};
use lazy_static::lazy_static;

type Job = Box<dyn FnOnce() + Send>;

thread_local! {
    static ON_RUNTIME_THREAD: Cell<bool> = const { Cell::new(false) };
}

lazy_static! {
    static ref JOBS: mpsc::UnboundedSender<Job> = {
        let (sender, mut receiver) = mpsc::unbounded::<Job>();
        thread::Builder::new()
            .name("srt-c-compio".into())
            .spawn(move || {
                ON_RUNTIME_THREAD.with(|f| f.set(true));
                let runtime =
                    compio::runtime::Runtime::new().expect("failed to create compio runtime");
                runtime.block_on(async move {
                    while let Some(job) = receiver.next().await {
                        job();
                    }
                });
            })
            .expect("failed to spawn compio runtime thread");
        sender
    };
}

pub fn init() {
    lazy_static::initialize(&JOBS);
}

/// Spawn a future (built on the runtime thread by `f`) onto the runtime, detached.
/// The returned receiver yields the output; dropping it does not cancel the task.
pub fn spawn<F, Fut>(f: F) -> oneshot::Receiver<Fut::Output>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future + 'static,
    Fut::Output: Send + 'static,
{
    let (sender, receiver) = oneshot::channel();
    let job: Job = Box::new(move || {
        compio::runtime::spawn(async move {
            let _ = sender.send(f().await);
        })
        .detach();
    });
    JOBS.unbounded_send(job)
        .expect("compio runtime thread has stopped");
    receiver
}

/// Run a future on the runtime thread and block the calling thread until it completes.
pub fn block_on<F, Fut>(f: F) -> Fut::Output
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future + 'static,
    Fut::Output: Send + 'static,
{
    assert!(
        !ON_RUNTIME_THREAD.with(|f| f.get()),
        "blocking on the srt-c runtime from within the runtime thread would deadlock"
    );
    futures::executor::block_on(spawn(f)).expect("srt-c runtime task panicked or was cancelled")
}

/// A timer driven by the runtime thread, awaitable from any thread.
pub async fn sleep(duration: Duration) {
    let _ = spawn(move || compio::time::sleep(duration)).await;
}
