#![warn(missing_docs)]
#![allow(clippy::field_reassign_with_default)]
//! Client library which exposes information from the different mayastor
//! control plane services through REST
//! Different versions are exposed through `versions`
//!
//! # Example:
//!
//! async fn main() {
//!     use rest_client::versions::v0::RestClient;
//!     let client = RestClient::new("https://localhost:8080");
//!     let _nodes = client.get_nodes().await.unwrap();
//! }

/// expose different versions of the client
pub mod versions;

use actix_web::{body::Body, client::Client};
use actix_web_opentelemetry::ClientExt;
use paperclip::actix::Apiv2Schema;
use serde::{Deserialize, Serialize};
use std::{io::BufReader, string::ToString};

/// Actix Rest Client
#[derive(Clone)]
pub struct ActixRestClient {
    client: actix_web::client::Client,
    url: String,
    trace: bool,
}

impl ActixRestClient {
    /// creates a new client which uses the specified `url`
    /// uses the rustls connector if the url has the https scheme
    pub fn new(url: &str, trace: bool) -> anyhow::Result<Self> {
        let url: url::Url = url.parse()?;

        match url.scheme() {
            "https" => Self::new_https(&url, trace),
            "http" => Ok(Self::new_http(&url, trace)),
            invalid => {
                let msg = format!("Invalid url scheme: {}", invalid);
                Err(anyhow::Error::msg(msg))
            }
        }
    }
    /// creates a new secure client
    fn new_https(url: &url::Url, trace: bool) -> anyhow::Result<Self> {
        let cert_file = &mut BufReader::new(
            &std::include_bytes!("../certs/rsa/ca.cert")[..],
        );

        let mut config = rustls::ClientConfig::new();
        config
            .root_store
            .add_pem_file(cert_file)
            .map_err(|_| anyhow::anyhow!("Add pem file to the root store!"))?;
        let connector = actix_web::client::Connector::new()
            .rustls(std::sync::Arc::new(config));
        let rest_client =
            Client::builder().connector(connector.finish()).finish();

        Ok(Self {
            client: rest_client,
            url: url.to_string().trim_end_matches('/').into(),
            trace,
        })
    }
    /// creates a new client
    fn new_http(url: &url::Url, trace: bool) -> Self {
        Self {
            client: Client::new(),
            url: url.to_string().trim_end_matches('/').into(),
            trace,
        }
    }
    async fn get_vec<R>(&self, urn: String) -> anyhow::Result<Vec<R>>
    where
        for<'de> R: Deserialize<'de>,
    {
        let uri = format!("{}{}", self.url, urn);

        let result = if self.trace {
            self.client.get(uri.clone()).trace_request().send().await
        } else {
            self.client.get(uri.clone()).send().await
        };

        let mut rest_response = result.map_err(|error| {
            anyhow::anyhow!(
                "Failed to get uri '{}' from rest, err={:?}",
                uri,
                error
            )
        })?;

        let rest_body = rest_response.body().await?;
        if rest_response.status().is_success() {
            match serde_json::from_slice(&rest_body) {
                Ok(result) => Ok(result),
                Err(_) => Ok(vec![serde_json::from_slice::<R>(&rest_body)?]),
            }
        } else {
            let error: serde_json::value::Value =
                serde_json::from_slice(&rest_body)?;
            Err(anyhow::anyhow!(error.to_string()))
        }
    }
    async fn put<R, B: Into<Body>>(
        &self,
        urn: String,
        body: B,
    ) -> anyhow::Result<R>
    where
        for<'de> R: Deserialize<'de>,
    {
        let uri = format!("{}{}", self.url, urn);

        let result = if self.trace {
            self.client
                .put(uri.clone())
                .content_type("application/json")
                .trace_request()
                .send_body(body)
                .await
        } else {
            self.client
                .put(uri.clone())
                .content_type("application/json")
                .send_body(body)
                .await
        };

        let mut rest_response = result.map_err(|error| {
            anyhow::anyhow!(
                "Failed to put uri '{}' from rest, err={:?}",
                uri,
                error
            )
        })?;

        let rest_body = rest_response.body().await?;
        if rest_response.status().is_success() {
            Ok(serde_json::from_slice::<R>(&rest_body)?)
        } else {
            let error: serde_json::value::Value =
                serde_json::from_slice(&rest_body)?;
            Err(anyhow::anyhow!(error.to_string()))
        }
    }
    async fn del<R>(&self, urn: String) -> anyhow::Result<R>
    where
        for<'de> R: Deserialize<'de>,
    {
        let uri = format!("{}{}", self.url, urn);

        let result = if self.trace {
            self.client.delete(uri.clone()).trace_request().send().await
        } else {
            self.client.delete(uri.clone()).send().await
        };

        let mut rest_response = result.map_err(|error| {
            anyhow::anyhow!(
                "Failed to delete uri '{}' from rest, err={:?}",
                uri,
                error
            )
        })?;

        let rest_body = rest_response.body().await?;
        if rest_response.status().is_success() {
            Ok(serde_json::from_slice::<R>(&rest_body)?)
        } else {
            let error: serde_json::value::Value =
                serde_json::from_slice(&rest_body)?;
            Err(anyhow::anyhow!(error.to_string()))
        }
    }
}

/// Generic JSON value eg: { "size": 1024 }
#[derive(Debug, Clone, Apiv2Schema)]
pub struct JsonGeneric {
    inner: serde_json::Value,
}
impl Serialize for JsonGeneric {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.inner.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for JsonGeneric {
    fn deserialize<D>(deserializer: D) -> Result<JsonGeneric, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        Ok(JsonGeneric::from(value))
    }
}
impl std::fmt::Display for JsonGeneric {
    /// Get inner JSON value as a string
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.inner.to_string())
    }
}
impl JsonGeneric {
    /// New JsonGeneric from a JSON value
    pub fn from(value: serde_json::Value) -> Self {
        Self {
            inner: value,
        }
    }
}
