//! The persistent store is used to save information that is required by
//! Mayastor across restarts.
//!
//! etcd is used as the backing store and is interacted with through the use of
//! the etcd-client crate. This crate has a dependency on the tokio async
//! runtime.
use crate::{
    core,
    core::Reactor,
    store::{
        etcd::Etcd,
        store_defs::{
            DeleteWait,
            GetWait,
            PutWait,
            Store,
            StoreError,
            StoreKey,
            StoreValue,
        },
    },
};
use futures::channel::oneshot;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use serde_json::Value;
use snafu::ResultExt;
use std::{future::Future, time::Duration};

/// Persistent store builder.
pub struct PersistentStoreBuilder {
    /// Default port.
    default_port: u16,
    /// Endpoint of the backing store.
    endpoint: Option<String>,
    /// Operation timeout.
    timeout: Duration,
    /// Number of operation retries.
    retries: u8,
    /// Interval duration.
    interval: Duration,
}

impl Default for PersistentStoreBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PersistentStoreBuilder {
    /// Creates new `PersistentStoreBuilder` instance.
    pub fn new() -> Self {
        Self {
            endpoint: None,
            default_port: 2379,
            timeout: Duration::from_secs(1),
            retries: 5,
            interval: Duration::from_secs(1),
        }
    }

    /// Sets the default port.
    pub fn with_default_port(mut self, port: u16) -> Self {
        self.default_port = port;
        self
    }

    /// Sets store's endpoint. Adds the default port to the endpoint if one
    /// isn't specified.
    pub fn with_endpoint(mut self, endpoint: &str) -> Self {
        self.endpoint = Some(match endpoint.contains(':') {
            true => endpoint.to_string(),
            false => format!("{endpoint}:{port}", port = self.default_port),
        });
        self
    }

    /// Sets operation timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets number of operation retries.
    pub fn with_retries(mut self, retries: u8) -> Self {
        self.retries = retries;
        self
    }

    /// Sets interval between operation retries.
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Consumes `PersistentStoreBuilder` instance and initialises the
    /// persistent store. If the supplied endpoint is 'None', the store is
    /// uninitalised and unavailable for use.
    pub async fn connect(self) {
        PersistentStore::connect(self).await
    }
}

/// Persistent store.
pub struct PersistentStore {
    /// Backing store used for persistence.
    store: Etcd,
    /// Endpoint of the backing store.
    endpoint: String,
    /// Operation timeout.
    timeout: Duration,
    /// Number of operation retries.
    retries: u8,
    /// Operation interval.
    interval: Duration,
}

/// Persistent store global instance.
static PERSISTENT_STORE: OnceCell<Mutex<PersistentStore>> = OnceCell::new();

impl PersistentStore {
    /// Initialises the persistent store.
    /// If the supplied endpoint is 'None', the store is uninitalised and
    /// unavailable for use.
    async fn connect(bld: PersistentStoreBuilder) {
        let Some(endpoint) = bld.endpoint else {
            // No endpoint means no persistent store.
            warn!("Persistent store not initialised");
            return;
        };

        let timeout = bld.timeout;
        let retries = bld.retries;
        let interval = bld.interval;
        let store = Self::connect_to_backing_store(&endpoint.clone()).await;

        info!(
            "Persistent store operation timeout: {timeout:?}, \
            number of retries: {retries}"
        );

        PERSISTENT_STORE.get_or_init(|| {
            Mutex::new(PersistentStore {
                store,
                endpoint,
                timeout,
                retries,
                interval,
            })
        });
    }

    /// Connects to etcd as the backing store.
    /// A connection to the store will be attempted continuously until
    /// successful. This is necessary as the backing store is essential to the
    /// operation of Mayastor across restarts.
    async fn connect_to_backing_store(endpoint: &str) -> Etcd {
        let mut output_err = true;
        loop {
            match Etcd::new(endpoint).await {
                Ok(store) => {
                    info!("Connected to etcd on endpoint {}", endpoint);
                    return store;
                }
                Err(_) => {
                    if output_err {
                        // Only output the error on first failure to prevent
                        // flooding the logs.
                        error!(
                            "Failed to connect to etcd on endpoint {}. Retrying...",
                            endpoint
                        );
                        output_err = false;
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    /// Puts a key-value in the store.
    pub async fn put(
        key: &impl StoreKey,
        value: &impl StoreValue,
    ) -> Result<(), StoreError> {
        let put_value = serde_json::to_value(value)
            .expect("Failed to convert value to a serde_json value");
        let key_string = key.to_string();
        let value_clone = put_value.clone();

        let rx = Self::execute_store_op(async move {
            info!(
                "Putting key {}, value {} in store.",
                key_string,
                value_clone.to_string()
            );

            match Self::backing_store()
                .put_kv(&key_string, &value_clone)
                .await
            {
                Ok(_) => {
                    info!(
                        "Successfully put key {}, value {} in store.",
                        key_string,
                        value_clone.to_string()
                    );
                    Ok(())
                }
                Err(e) => Err(e),
            }
        });

        rx.await.context(PutWait {
            key: key.to_string(),
            value: put_value.to_string(),
        })?
    }

    /// Retrieves a value, with the given key, from the store.
    pub async fn get(key: &impl StoreKey) -> Result<Value, StoreError> {
        let key_string = key.to_string();
        let rx = Self::execute_store_op(async move {
            info!("Getting key {} from store.", key_string);
            match Self::backing_store().get_kv(&key_string).await {
                Ok(value) => {
                    info!("Successfully got key {}", key_string);
                    Ok(value)
                }
                Err(e) => Err(e),
            }
        });
        rx.await.context(GetWait {
            key: key.to_string(),
        })?
    }

    /// Deletes the entry in the store with the given key.
    pub async fn delete(key: &impl StoreKey) -> Result<(), StoreError> {
        let key_string = key.to_string();
        let rx = Self::execute_store_op(async move {
            info!("Deleting key {} from store.", key_string);
            match Self::backing_store().delete_kv(&key_string).await {
                Ok(_) => {
                    info!(
                        "Successfully deleted key {} from store.",
                        key_string
                    );
                    Ok(())
                }
                Err(e) => Err(e),
            }
        });
        rx.await.context(DeleteWait {
            key: key.to_string(),
        })?
    }

    /// Executes a future representing a store operation (i.e. put, get, delete)
    /// on the tokio runtime.
    /// A channel is returned which is signalled when the operation completes.
    /// If an operation times out, reconnect to the backing store before failing
    /// the operation.
    fn execute_store_op<T: 'static + Send>(
        f: impl Future<Output = Result<T, StoreError>> + Send + 'static,
    ) -> oneshot::Receiver<Result<T, StoreError>> {
        let (tx, rx) = oneshot::channel::<Result<T, StoreError>>();
        core::runtime::spawn(async move {
            let op_timeout = Self::timeout();
            let result = match tokio::time::timeout(op_timeout, f).await {
                Ok(result) => result,
                Err(_) => {
                    Self::reconnect().await;
                    Err(StoreError::OpTimeout {})
                }
            };

            // Execute the sending of the result on a "Mayastor thread".
            let rx = Reactor::spawn_at_primary(async move {
                if tx.send(result).is_err() {
                    tracing::error!(
                        "Failed to send completion for 'put' request."
                    );
                }
            })
            .expect("Failed to send future to Mayastor thread");
            let _ = rx.await;
        });
        rx
    }

    /// Determines if the persistent store has been enabled.
    pub fn enabled() -> bool {
        PERSISTENT_STORE.get().is_some()
    }

    /// Gets the persistent store instance.
    fn instance() -> &'static Mutex<PersistentStore> {
        PERSISTENT_STORE
            .get()
            .expect("Persistent store should have been initialised")
    }

    /// Gets an instance of the backing store.
    fn backing_store() -> Etcd {
        Self::instance().lock().store.clone()
    }

    /// Gets the endpoint of the backing store.
    pub fn endpoint() -> String {
        Self::instance().lock().endpoint.clone()
    }

    /// Gets the operation timeout.
    pub fn timeout() -> Duration {
        Self::instance().lock().timeout
    }

    /// Gets the operation interval.
    pub fn interval() -> Duration {
        Self::instance().lock().interval
    }

    /// Gets the number of operation retries.
    pub fn retries() -> u8 {
        Self::instance().lock().retries
    }

    /// Reconnects to the backing store and replaces the old connection with the
    /// new connection.
    async fn reconnect() {
        warn!("Attempting to reconnect to persistent store....");
        let persistent_store = Self::instance();
        let backing_store =
            Self::connect_to_backing_store(&PersistentStore::endpoint()).await;
        persistent_store.lock().store = backing_store;
    }
}
