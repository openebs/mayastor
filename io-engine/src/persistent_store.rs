//! The persistent store is used to save information that is required by
//! Mayastor across restarts.
//!
//! etcd is used as the backing store and is interacted with through the use of
//! the etcd-client crate. This crate has a dependency on the tokio async
//! runtime.
use crate::{
    core,
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
use serde_json::Value;
use snafu::ResultExt;
use std::{future::Future, sync::Mutex, time::Duration};

static DEFAULT_PORT: &str = "2379";
static STORE_OP_TIMEOUT: Duration = Duration::from_secs(30);
static PERSISTENT_STORE: OnceCell<Option<Mutex<PersistentStore>>> =
    OnceCell::new();

/// Persistent store
pub struct PersistentStore {
    /// Backing store used for persistence.
    store: Etcd,
    /// Endpoint of the backing store.
    endpoint: String,
}

impl PersistentStore {
    /// Initialise the persistent store.
    /// If the supplied endpoint is 'None', the store is uninitalised and
    /// unavailable for use.
    pub async fn init(endpoint: Option<String>) {
        if endpoint.is_none() {
            // No endpoint means no persistent store.
            warn!("Persistent store not initialised");
            return;
        }

        assert!(endpoint.is_some());

        // An endpoint has been provided, initialise the persistent store.
        let endpoint = Self::format_endpoint(&endpoint.unwrap());
        let store = Self::connect_to_backing_store(&endpoint.clone()).await;
        PERSISTENT_STORE.get_or_init(|| {
            Some(Mutex::new(PersistentStore {
                store,
                endpoint,
            }))
        });
    }

    /// Adds the default port to the endpoint if one isn't already specified.
    fn format_endpoint(endpoint: &str) -> String {
        match endpoint.contains(':') {
            true => endpoint.to_string(),
            false => format!("{}:{}", endpoint, DEFAULT_PORT),
        }
    }

    /// Connect to etcd as the backing store.
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

    /// Put a key-value in the store.
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

    /// Retrieve a value, with the given key, from the store.
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

    /// Delete the entry in the store with the given key.
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
            let result = match tokio::time::timeout(STORE_OP_TIMEOUT, f).await {
                Ok(result) => result,
                Err(_) => {
                    Self::reconnect().await;
                    Err(StoreError::OpTimeout {})
                }
            };

            // Execute the sending of the result on a "Mayastor thread".
            let thread = core::Mthread::get_init();
            let rx = thread
                .spawn_local(async move {
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

    /// Determine if the persistent store has been enabled.
    pub fn enabled() -> bool {
        PERSISTENT_STORE.get().is_some()
    }

    /// Get the persistent store.
    fn new() -> &'static Mutex<PersistentStore> {
        PERSISTENT_STORE
            .get()
            .expect("Persistent store should have been initialised")
            .as_ref()
            .expect("Failed to get persistent store")
    }

    /// Get an instance of the backing store.
    fn backing_store() -> Etcd {
        Self::new().lock().unwrap().store.clone()
    }

    /// Get the endpoint of the backing store.
    fn endpoint() -> String {
        Self::new().lock().unwrap().endpoint.clone()
    }

    /// Reconnects to the backing store and replaces the old connection with the
    /// new connection.
    async fn reconnect() {
        warn!("Attempting to reconnect to persistent store....");
        let persistent_store = Self::new();
        let backing_store =
            Self::connect_to_backing_store(&PersistentStore::endpoint()).await;
        persistent_store.lock().unwrap().store = backing_store;
    }
}
