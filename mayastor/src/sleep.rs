use crate::core::{runtime::spawn, Mthread};
use futures::channel::oneshot;
use std::time::Duration;

/// Async sleep that can be called from Mayastor.
/// A sleep is scheduled on the tokio runtime and a receiver channel returned
/// which is signalled once the sleep completes.
/// The sleep duration is not exact as it does not account for thread scheduling
/// but it should be sufficient for most cases.
pub(crate) fn mayastor_sleep(duration: Duration) -> oneshot::Receiver<()> {
    let (tx, rx) = oneshot::channel::<()>();
    spawn(async move {
        tokio::time::sleep(duration).await;
        let thread = Mthread::get_init();
        let rx = thread
            .spawn_local(async move {
                if tx.send(()).is_err() {
                    tracing::error!(
                        "Failed to send completion for Mayastor sleep."
                    );
                }
            })
            .unwrap();
        let _ = rx.await;
    });
    rx
}
