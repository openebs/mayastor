//! Definition of a trait for a key-value store together with its error codes.

use async_trait::async_trait;
use etcd_client::Error;
use serde_json::{Error as SerdeError, Value};
use snafu::Snafu;

/// Definition of errors that can be returned from the key-value store.
#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum StoreError {
    /// Failed to connect to the key-value store.
    #[snafu(display("Failed to connect to store. Error {}", source))]
    Connect { source: Error },
    /// Failed to 'put' an entry in the store.
    #[snafu(display(
        "Failed to 'put' entry with key {} and value {}. Error {}",
        key,
        value,
        source
    ))]
    Put {
        key: String,
        value: String,
        source: Error,
    },
    /// Failed to wait for 'put' operation.
    #[snafu(display(
    "Failed to wait for 'put' operation to complete for key {} and value {:?}.",
    key,
    value,
    ))]
    PutWait {
        key: String,
        value: String,
        source: futures::channel::oneshot::Canceled,
    },
    /// Failed to 'get' an entry from the store.
    #[snafu(display(
        "Failed to 'get' entry with key {}. Error {}",
        key,
        source
    ))]
    Get { key: String, source: Error },
    /// Failed to wait for 'get' operation.
    #[snafu(display(
        "Failed to wait for 'get' operation to complete for key {}.",
        key,
    ))]
    GetWait {
        key: String,
        source: futures::channel::oneshot::Canceled,
    },
    /// Failed to find an entry with the given key.
    #[snafu(display("Entry with key {} not found.", key))]
    MissingEntry { key: String },
    /// Failed to 'delete' an entry from the store.
    #[snafu(display(
        "Failed to 'delete' entry with key {}. Error {}",
        key,
        source
    ))]
    Delete { key: String, source: Error },
    /// Failed to wait for 'delete' operation.
    #[snafu(display(
        "Failed to wait for 'delete' operation to complete for key {}.",
        key,
    ))]
    DeleteWait {
        key: String,
        source: futures::channel::oneshot::Canceled,
    },
    /// Failed to 'watch' an entry in the store.
    #[snafu(display(
        "Failed to 'watch' entry with key {}. Error {}",
        key,
        source
    ))]
    Watch { key: String, source: Error },
    /// Empty key.
    #[snafu(display("Failed to get key as string. Error {}", source))]
    KeyString { source: Error },
    /// Empty value.
    #[snafu(display("Failed to get value as string. Error {}", source))]
    ValueString { source: Error },
    /// Failed to deserialise value.
    #[snafu(display(
        "Failed to deserialise value {}. Error {}",
        value,
        source
    ))]
    DeserialiseValue { value: String, source: SerdeError },
    /// Failed to serialise value.
    #[snafu(display("Failed to serialise value. Error {}", source))]
    SerialiseValue { source: SerdeError },
    /// Operation timed out.
    #[snafu(display("Store operation timed out.",))]
    OpTimeout {},
}

/// Store keys type trait
pub trait StoreKey: Sync + ToString + std::fmt::Debug {}
impl<T> StoreKey for T where T: Sync + ToString + std::fmt::Debug {}
/// Store value type trait
pub trait StoreValue: Sync + serde::Serialize + std::fmt::Debug {}
impl<T> StoreValue for T where T: Sync + serde::Serialize + std::fmt::Debug {}

/// Trait defining the operations that can be performed on a key-value store.
#[async_trait]
pub trait Store: Sync + Send + Clone {
    /// Put entry into the store.
    async fn put_kv<K: StoreKey, V: StoreValue>(
        &mut self,
        key: &K,
        value: &V,
    ) -> Result<(), StoreError>;

    /// Get an entry from the store.
    async fn get_kv<K: StoreKey>(
        &mut self,
        key: &K,
    ) -> Result<Value, StoreError>;

    /// Delete an entry from the store.
    async fn delete_kv<K: StoreKey>(
        &mut self,
        key: &K,
    ) -> Result<(), StoreError>;

    /// Identify whether or not the store is online.
    async fn online(&mut self) -> bool;
}
