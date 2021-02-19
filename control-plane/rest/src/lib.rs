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

use actix_web::{
    body::Body,
    client::{
        Client,
        ClientBuilder,
        ClientResponse,
        PayloadError,
        SendRequestError,
    },
    dev::ResponseHead,
    web::Bytes,
};
use actix_web_opentelemetry::ClientExt;
use futures::Stream;
use paperclip::actix::Apiv2Schema;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
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
        Self::new_timeout(url, trace, std::time::Duration::from_secs(5))
    }
    /// creates a new client which uses the specified `url`
    /// uses the rustls connector if the url has the https scheme
    pub fn new_timeout(
        url: &str,
        trace: bool,
        timeout: std::time::Duration,
    ) -> anyhow::Result<Self> {
        let url: url::Url = url.parse()?;
        let builder = Client::builder().timeout(timeout);

        match url.scheme() {
            "https" => Self::new_https(builder, &url, trace),
            "http" => Ok(Self::new_http(builder, &url, trace)),
            invalid => {
                let msg = format!("Invalid url scheme: {}", invalid);
                Err(anyhow::Error::msg(msg))
            }
        }
    }
    /// creates a new secure client
    fn new_https(
        client: ClientBuilder,
        url: &url::Url,
        trace: bool,
    ) -> anyhow::Result<Self> {
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
        let rest_client = client.connector(connector.finish()).finish();

        Ok(Self {
            client: rest_client,
            url: url.to_string().trim_end_matches('/').into(),
            trace,
        })
    }
    /// creates a new client
    fn new_http(client: ClientBuilder, url: &url::Url, trace: bool) -> Self {
        Self {
            client: client.finish(),
            url: url.to_string().trim_end_matches('/').into(),
            trace,
        }
    }
    async fn get_vec<R>(&self, urn: String) -> ClientResult<Vec<R>>
    where
        for<'de> R: Deserialize<'de>,
    {
        let uri = format!("{}{}", self.url, urn);

        let result = if self.trace {
            self.client.get(uri.clone()).trace_request().send().await
        } else {
            self.client.get(uri.clone()).send().await
        };

        let rest_response = result.context(Send {
            details: format!("Failed to get_vec uri {}", uri),
        })?;

        Self::rest_vec_result(rest_response).await
    }
    async fn put<R, B: Into<Body>>(
        &self,
        urn: String,
        body: B,
    ) -> Result<R, ClientError>
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

        let rest_response = result.context(Send {
            details: format!("Failed to put uri {}", uri),
        })?;

        Self::rest_result(rest_response).await
    }
    async fn del<R>(&self, urn: String) -> ClientResult<R>
    where
        for<'de> R: Deserialize<'de>,
    {
        let uri = format!("{}{}", self.url, urn);

        let result = if self.trace {
            self.client.delete(uri.clone()).trace_request().send().await
        } else {
            self.client.delete(uri.clone()).send().await
        };

        let rest_response = result.context(Send {
            details: format!("Failed to delete uri {}", uri),
        })?;

        Self::rest_result(rest_response).await
    }

    async fn rest_vec_result<S, R>(
        mut rest_response: ClientResponse<S>,
    ) -> ClientResult<Vec<R>>
    where
        S: Stream<Item = Result<Bytes, PayloadError>> + Unpin,
        for<'de> R: Deserialize<'de>,
    {
        let status = rest_response.status();
        let headers = rest_response.headers().clone();
        let head = || {
            let mut head = ResponseHead::new(status);
            head.headers = headers.clone();
            head
        };
        let body = rest_response.body().await.map_err(|_| {
            ClientError::InvalidPayload {
                head: head(),
            }
        })?;
        if status.is_success() {
            match serde_json::from_slice(&body) {
                Ok(r) => Ok(r),
                Err(_error) => match serde_json::from_slice(&body) {
                    Ok(r) => Ok(vec![r]),
                    Err(_error) => Err(ClientError::InvalidBody {
                        head: head(),
                        body,
                    }),
                },
            }
        } else if body.is_empty() {
            Err(ClientError::Header {
                head: head(),
            })
        } else {
            let error = serde_json::from_slice::<serde_json::Value>(&body)
                .map_err(|_| ClientError::InvalidBody {
                    head: head(),
                    body,
                })?;
            Err(ClientError::Valid {
                head: head(),
                error,
            })
        }
    }

    async fn rest_result<S, R>(
        mut rest_response: ClientResponse<S>,
    ) -> Result<R, ClientError>
    where
        S: Stream<Item = Result<Bytes, PayloadError>> + Unpin,
        for<'de> R: Deserialize<'de>,
    {
        let status = rest_response.status();
        let headers = rest_response.headers().clone();
        let head = || {
            let mut head = ResponseHead::new(status);
            head.headers = headers.clone();
            head
        };
        let body = rest_response.body().await.map_err(|_| {
            ClientError::InvalidPayload {
                head: head(),
            }
        })?;
        if status.is_success() {
            let result = serde_json::from_slice(&body).map_err(|_| {
                ClientError::InvalidBody {
                    head: head(),
                    body,
                }
            })?;
            Ok(result)
        } else if body.is_empty() {
            Err(ClientError::Header {
                head: head(),
            })
        } else {
            let error = serde_json::from_slice::<serde_json::Value>(&body)
                .map_err(|_| ClientError::InvalidBody {
                    head: head(),
                    body,
                })?;
            Err(ClientError::Valid {
                head: head(),
                error,
            })
        }
    }
}

/// Result of a Rest Client Operation
/// T is the Object parsed from the Json body
pub type ClientResult<T> = Result<T, ClientError>;

/// Rest Client Error
#[derive(Debug, Snafu)]
pub enum ClientError {
    /// Failed to send message to the server
    #[snafu(display("{}, reason: {}", details, source))]
    Send {
        /// Message
        details: String,
        /// Source Request Error
        source: SendRequestError,
    },
    /// Invalid Resource Filter
    #[snafu(display("Invalid Resource Filter: {}", details))]
    InvalidFilter {
        /// Message
        details: String,
    },
    /// Invalid Payload
    #[snafu(display("Invalid payload, header: {:?}", head))]
    InvalidPayload {
        /// http Header
        head: ResponseHead,
    },
    /// Invalid Body
    #[snafu(display("Invalid body, header: {:?}", head))]
    InvalidBody {
        /// http Header
        head: ResponseHead,
        /// http Body
        body: Bytes,
    },
    /// No Body
    #[snafu(display("No body, header: {:?}", head))]
    Header {
        /// http Header
        head: ResponseHead,
    },
    /// Body in JSON format
    #[snafu(display("Http status: {}, error: {}", head.status, error.to_string()))]
    Valid {
        /// http Header
        head: ResponseHead,
        /// JSON error
        error: serde_json::Value,
    },
}

impl ClientError {
    fn filter(message: &str) -> ClientError {
        ClientError::InvalidFilter {
            details: message.to_string(),
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

    /// Get inner value
    pub fn into_inner(self) -> serde_json::Value {
        self.inner
    }
}
