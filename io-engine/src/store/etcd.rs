//! Implementation of an etcd key-value store.

use crate::store::store_defs::{
    Connect, Delete, DeserialiseValue, Get, Put, SerialiseValue, Store, StoreError,
    StoreError::MissingEntry, StoreKey, StoreValue, Txn as TxnErr, ValueString,
};
use async_trait::async_trait;
use etcd_client::{Client, Compare, CompareOp, Error, Txn, TxnOp, TxnOpResponse};
use serde_json::Value;
use snafu::ResultExt;

/// etcd client
#[derive(Clone)]
pub struct Etcd(Client);

impl std::fmt::Debug for Etcd {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl Etcd {
    /// Create a new instance of the etcd client
    pub async fn new(endpoint: &str) -> Result<Etcd, StoreError> {
        Ok(Self(
            Client::connect([endpoint], None)
                .await
                .context(Connect {})?,
        ))
    }
}

#[async_trait]
impl Store for Etcd {
    /// 'Put' a key-value pair into etcd.
    async fn put_kv<K: StoreKey, V: StoreValue>(
        &mut self,
        key: &K,
        value: &V,
    ) -> Result<(), StoreError> {
        let vec_value = serde_json::to_vec(value).context(SerialiseValue)?;
        self.0
            .put(key.to_string(), vec_value, None)
            .await
            .context(Put {
                key: key.to_string(),
                value: serde_json::to_string(value).unwrap(),
            })?;
        Ok(())
    }

    /// Executes a transaction for the given key. If the compare succeed, then
    /// new_value `Put` will be executed atomically, otherwise the current value
    /// will be `Get` and returned.
    async fn put_kv_cas<K: StoreKey>(
        &mut self,
        key: &K,
        new_value: Vec<u8>,
        expected_value: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        let cmp = Compare::value(key.to_string(), CompareOp::Equal, expected_value);
        let put_op = TxnOp::put(key.to_string(), new_value, None);
        let or_else_op = TxnOp::get(key.to_string(), None);

        let txn_resp = self
            .0
            .txn(
                Txn::new()
                    .when([cmp])
                    .and_then([put_op])
                    .or_else([or_else_op]),
            )
            .await
            .context(TxnErr {
                key: key.to_string(),
            })?;

        if !txn_resp.succeeded() {
            if let Some(TxnOpResponse::Get(g)) = &txn_resp.op_responses().first() {
                if let Some(kv) = g.kvs().first() {
                    let current_value = kv.value();
                    return Ok(Some(current_value.to_owned()));
                }
            } else {
                return Err(StoreError::Txn {
                    key: key.to_string(),
                    source: Error::IoError(std::io::Error::other(
                        "Requested TxnOpResponse::Get not found",
                    )),
                });
            }
        }

        Ok(None)
    }

    /// 'Get' the value for the given key from etcd.
    async fn get_kv<K: StoreKey>(&mut self, key: &K) -> Result<Value, StoreError> {
        let resp = self.0.get(key.to_string(), None).await.context(Get {
            key: key.to_string(),
        })?;
        match resp.kvs().first() {
            Some(kv) => Ok(
                serde_json::from_slice(kv.value()).context(DeserialiseValue {
                    value: kv.value_str().context(ValueString {})?,
                })?,
            ),
            None => Err(MissingEntry {
                key: key.to_string(),
            }),
        }
    }

    /// 'Delete' the entry with the given key from etcd.
    async fn delete_kv<K: StoreKey>(&mut self, key: &K) -> Result<(), StoreError> {
        self.0.delete(key.to_string(), None).await.context(Delete {
            key: key.to_string(),
        })?;
        Ok(())
    }

    async fn online(&mut self) -> bool {
        self.0.status().await.is_ok()
    }
}
