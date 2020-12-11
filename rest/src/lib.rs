#![warn(missing_docs)]
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
use serde::Deserialize;
use std::{io::BufReader, string::ToString};

/// Actix Rest Client
#[derive(Clone)]
pub struct ActixRestClient {
    client: actix_web::client::Client,
    url: String,
}

impl ActixRestClient {
    /// creates a new client which uses the specified `url`
    pub fn new(url: &str) -> anyhow::Result<Self> {
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
            url: url.to_string(),
        })
    }
    async fn get_vec<R>(&self, urn: String) -> anyhow::Result<Vec<R>>
    where
        for<'de> R: Deserialize<'de>,
    {
        let uri = format!("{}{}", self.url, urn);

        let mut rest_response =
            self.client.get(uri.clone()).send().await.map_err(|error| {
                anyhow::anyhow!(
                    "Failed to get uri '{}' from rest, err={:?}",
                    uri,
                    error
                )
            })?;

        let rest_body = rest_response.body().await?;
        match serde_json::from_slice(&rest_body) {
            Ok(result) => Ok(result),
            Err(_) => Ok(vec![serde_json::from_slice::<R>(&rest_body)?]),
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

        let mut rest_response = self
            .client
            .put(uri.clone())
            .content_type("application/json")
            .send_body(body)
            .await
            .map_err(|error| {
                anyhow::anyhow!(
                    "Failed to put uri '{}' from rest, err={:?}",
                    uri,
                    error
                )
            })?;

        let rest_body = rest_response.body().await?;
        Ok(serde_json::from_slice::<R>(&rest_body)?)
    }
    async fn del<R>(&self, urn: String) -> anyhow::Result<R>
    where
        for<'de> R: Deserialize<'de>,
    {
        let uri = format!("{}{}", self.url, urn);

        let mut rest_response = self
            .client
            .delete(uri.clone())
            .send()
            .await
            .map_err(|error| {
                anyhow::anyhow!(
                    "Failed to delete uri '{}' from rest, err={:?}",
                    uri,
                    error
                )
            })?;

        let rest_body = rest_response.body().await?;
        Ok(serde_json::from_slice::<R>(&rest_body)?)
    }
}
