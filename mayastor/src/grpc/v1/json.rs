//!
//! gRPC method to proxy calls to (local) SPDK json-rpc service

use crate::grpc::GrpcResult;
use ::rpc::mayastor::v1::json::{JsonRpc, JsonRpcRequest, JsonRpcResponse};
use jsonrpc::error::Error;
use std::borrow::Cow;
use tonic::{Request, Response};

/// RPC Service for local SPDK json-rpc calls
#[derive(Debug)]
pub struct JsonService {
    // FIXME: using a static lifetime here is not ideal
    rpc_addr: Cow<'static, str>,
}

impl JsonService {
    pub fn new(rpc_addr: Cow<'static, str>) -> Self {
        Self {
            rpc_addr,
        }
    }
}

#[tonic::async_trait]
impl JsonRpc for JsonService {
    /// Invoke a json-rpc method and return the result
    #[tracing::instrument(skip(self))]
    async fn json_rpc_call(
        &self,
        request: Request<JsonRpcRequest>,
    ) -> GrpcResult<JsonRpcResponse> {
        let args = request.into_inner();

        let result = self
            .spdk_jsonrpc_call(&args.method, empty_as_none(&args.params))
            .await?;

        Ok(Response::new(JsonRpcResponse {
            result,
        }))
    }
}

fn empty_as_none(value: &str) -> Option<&str> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

impl JsonService {
    async fn spdk_jsonrpc_call(
        &self,
        method: &str,
        arg: Option<&str>,
    ) -> Result<String, Error> {
        let params: Option<serde_json::Value> =
            arg.map(serde_json::from_str).transpose()?;

        let result: serde_json::Value =
            jsonrpc::call(&self.rpc_addr, method, params).await?;

        serde_json::to_string_pretty(&result).map_err(Error::ParseError)
    }
}
