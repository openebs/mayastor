use lazy_static::lazy_static;
use std::{
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::{signal::unix::SignalKind, sync::oneshot};

/// Returns a future that completes when a shutdown event has been received.
/// Shutdown events: INT|TERM.
pub async fn wait() {
    let _ = Shutdown::wait().await;
}

type ShutdownSync = Arc<Mutex<Shutdown>>;
/// Shutdown Event handler.
struct Shutdown {
    /// Listeners awaiting for the shutdown.
    listeners: Vec<oneshot::Sender<SignalKind>>,
    /// Whether we've already received the shutdown signal and which signal
    /// triggered it.
    shutdown: Option<SignalKind>,
}
impl Shutdown {
    /// Get a sync wrapper of `Self`.
    /// The internal task to listen on the shutdown event is scheduled.
    fn new_sync(event: impl ShutdownEvent + 'static) -> ShutdownSync {
        let this = Arc::new(Mutex::new(Self::new()));
        let this_clone = this.clone();
        tokio::spawn(async move { Self::run(this_clone, event).await });
        this
    }
    fn new() -> Self {
        Shutdown {
            listeners: Vec::new(),
            shutdown: None,
        }
    }
    /// Get a shutdown channel to await on or None if the shutdown event has
    /// already been received.
    fn shutdown_chan(
        &mut self,
    ) -> Result<oneshot::Receiver<SignalKind>, SignalKind> {
        if let Some(event) = self.shutdown {
            Err(event)
        } else {
            let (send, receive) = oneshot::channel();
            self.listeners.push(send);
            Ok(receive)
        }
    }
    /// Run the main waiting loop that waits for the reception of SIGINT or
    /// SIGTERM. When any of them is received the listeners are notified.
    async fn run(this: ShutdownSync, event: impl ShutdownEvent) {
        let kind = event.wait().await;

        let mut this = this.lock().expect("not poisoned");
        this.shutdown = Some(kind);

        for sender in std::mem::take(&mut this.listeners) {
            // It's ok if the receiver has already been dropped.
            sender.send(kind).ok();
        }
    }
    /// Returns a future that completes when a shutdown event has been received.
    /// The output of the future is signal which triggered the shutdown.
    /// None is returned if we failed to receive the event from the internal
    /// task. Shutdown events: INT|TERM.
    fn wait() -> impl Future<Output = Option<SignalKind>> {
        Self::wait_int_term()
    }
    fn wait_int_term() -> impl Future<Output = Option<SignalKind>> {
        lazy_static! {
            static ref TERM: ShutdownSync = Shutdown::new_sync(IntTermEvent {});
        }
        let chan = TERM.lock().expect("not poisoned").shutdown_chan();
        async move {
            match chan {
                Ok(wait) => wait.await.ok(),
                Err(signal) => Some(signal),
            }
        }
    }
}

/// Internal Shutdown Event which returns which signal triggered it.
#[async_trait::async_trait]
trait ShutdownEvent: Send + Sync {
    async fn wait(&self) -> SignalKind;
}

/// Shutdown Event when INT | TERM are received.
struct IntTermEvent {}
#[async_trait::async_trait]
impl ShutdownEvent for IntTermEvent {
    async fn wait(&self) -> SignalKind {
        let mut sig_int = tokio::signal::unix::signal(SignalKind::interrupt())
            .expect("to register SIGINT");
        let mut sig_term = tokio::signal::unix::signal(SignalKind::terminate())
            .expect("to register SIGTERM");

        let kind = tokio::select! {
            _ = sig_int.recv() => {
                tracing::warn!(signal = ?SignalKind::interrupt(), "Signalled");
                SignalKind::interrupt()
            },
            _ = sig_term.recv() => {
                tracing::warn!(signal = ?SignalKind::terminate(), "Signalled");
                SignalKind::terminate()
            },
        };

        kind
    }
}

#[cfg(test)]
mod tests {
    use crate::nodeplugin_grpc::mayastor_node_plugin::{
        mayastor_node_plugin_client::MayastorNodePluginClient,
        mayastor_node_plugin_server::{
            MayastorNodePlugin,
            MayastorNodePluginServer,
        },
        FindVolumeReply,
        FindVolumeRequest,
        FreezeFsReply,
        FreezeFsRequest,
        UnfreezeFsReply,
        UnfreezeFsRequest,
        VolumeType,
    };
    use std::{
        str::FromStr,
        sync::{Arc, Mutex},
        time::Duration,
    };
    use tonic::{
        transport::{Server, Uri},
        Code,
        Request,
        Response,
        Status,
    };

    #[derive(Debug, Default)]
    struct MayastorNodePluginSvc {
        first_call: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    }
    impl MayastorNodePluginSvc {
        fn new(chan: tokio::sync::oneshot::Sender<()>) -> Self {
            Self {
                first_call: Arc::new(Mutex::new(Some(chan))),
            }
        }
    }

    #[tonic::async_trait]
    impl MayastorNodePlugin for MayastorNodePluginSvc {
        async fn freeze_fs(
            &self,
            _request: Request<FreezeFsRequest>,
        ) -> Result<Response<FreezeFsReply>, Status> {
            unimplemented!()
        }

        async fn unfreeze_fs(
            &self,
            _request: Request<UnfreezeFsRequest>,
        ) -> Result<Response<UnfreezeFsReply>, Status> {
            unimplemented!()
        }

        async fn find_volume(
            &self,
            _request: Request<FindVolumeRequest>,
        ) -> Result<Response<FindVolumeReply>, Status> {
            {
                let mut inner = self.first_call.lock().unwrap();
                let sender = inner
                    .take()
                    .expect("Only the first call should get through!");
                sender.send(()).unwrap();
            }
            println!("Sleeping...");
            tokio::time::sleep(Duration::from_secs(2)).await;
            println!("Done...");
            Ok(Response::new(FindVolumeReply {
                volume_type: VolumeType::Rawblock as i32,
            }))
        }
    }

    /// Tests the shutdown of a tonic service.
    /// A shutdown event is issued after a "long" requests starts being
    /// processed. This request should be able to complete, even if it takes
    /// longer. However, meanwhile, any new requests should be rejected.
    #[tokio::test]
    async fn shutdown() {
        async fn wait(wait: tokio::sync::oneshot::Receiver<()>) {
            wait.await.unwrap();
        }
        let (first_sender, first_receiver) = tokio::sync::oneshot::channel();
        let (shutdown_sender, shutdown_receiver) =
            tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            if let Err(e) = Server::builder()
                .add_service(MayastorNodePluginServer::new(
                    MayastorNodePluginSvc::new(first_sender),
                ))
                .serve_with_shutdown(
                    "0.0.0.0:50011".parse().unwrap(),
                    wait(shutdown_receiver),
                )
                .await
            {
                panic!("gRPC server failed with error: {}", e);
            }
        });
        tokio::time::sleep(Duration::from_millis(250)).await;
        let channel = tonic::transport::Endpoint::from(
            Uri::from_str("https://0.0.0.0:50011").unwrap(),
        )
        .connect()
        .await
        .unwrap();
        let mut cli = MayastorNodePluginClient::new(channel);

        // 1. schedule the first request
        let mut cli_first = cli.clone();
        let first_request = tokio::spawn(async move {
            let response = cli_first
                .find_volume(FindVolumeRequest {
                    volume_id: "".to_string(),
                })
                .await;
            response
        });
        // 2. wait until the first request is being processed
        first_receiver.await.unwrap();

        // 3. send the shutdown event
        shutdown_sender.send(()).unwrap();
        // add a little slack so the shutdown can be processed
        tokio::time::sleep(Duration::from_millis(250)).await;

        // 4. try to send a new request which...
        let second_response = cli
            .find_volume(FindVolumeRequest {
                volume_id: "".to_string(),
            })
            .await;
        println!("Second Request Status: {:?}", second_response);
        // should fail because we've started to shutdown!
        assert_eq!(second_response.unwrap_err().code(), Code::Unknown);

        // 4. the initial request should complete gracefully
        let first_request_resp = first_request.await.unwrap();
        println!("First Request Response: {:?}", first_request_resp);
        assert_eq!(
            first_request_resp.unwrap().into_inner().volume_type,
            VolumeType::Rawblock as i32
        );
    }
}
