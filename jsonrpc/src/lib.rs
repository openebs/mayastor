//! json-rpc protocol over unix domain socket implementation as described
//! in spec: https://www.jsonrpc.org/specification.

extern crate nix;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
#[macro_use]
extern crate log;

pub mod error;
#[cfg(test)]
mod test;

use self::error::{Error, RpcCode};
use nix::errno::Errno;
use std::net::Shutdown;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
};

#[derive(Debug, Serialize, Deserialize)]
/// A JSONRPC request object
pub struct Request<'a> {
    /// The name of the RPC call
    pub method: &'a str,
    /// Parameters to the RPC call
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
    /// Identifier for this Request, which should appear in the response
    pub id: serde_json::Value,
    /// jsonrpc field, MUST be "2.0"
    pub jsonrpc: Option<&'a str>,
}

#[derive(Debug, Serialize, Deserialize)]
/// A JSONRPC response object
pub struct Response {
    /// A result if there is one, or null
    pub result: Option<serde_json::Value>,
    /// An error if there is one, or null
    pub error: Option<RpcError>,
    /// Identifier for this Request, which should match that of the request
    pub id: serde_json::Value,
    /// jsonrpc field, MUST be "2.0"
    pub jsonrpc: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
/// A JSONRPC error object
pub struct RpcError {
    /// The integer identifier of the error
    pub code: i32,
    /// A string describing the error
    pub message: String,
    /// Additional data specific to the error
    pub data: Option<serde_json::Value>,
}

/// Make json-rpc request and parse reply and return user data to caller.
pub async fn call<A, R>(
    sock_path: &str,
    method: &str,
    args: Option<A>,
) -> Result<R, Error>
where
    A: serde::ser::Serialize,
    R: 'static + serde::de::DeserializeOwned + Send,
{
    let params = match args {
        Some(val) => Some(serde_json::to_value(val).unwrap()),
        None => None,
    };

    let request = Request {
        method,
        params,
        id: From::from(0),
        jsonrpc: Some("2.0"),
    };

    let mut buf = serde_json::to_vec(&request)?;
    let sock = sock_path.to_string();
    let mut socket = UnixStream::connect(sock).await?;

    trace!("JSON request: {}", String::from_utf8_lossy(&buf));

    socket.write_all(&buf).await?;
    socket.shutdown(Shutdown::Write)?;

    buf.clear();
    socket.read_to_end(&mut buf).await?;
    socket.shutdown(Shutdown::Both)?;

    match parse_reply::<R>(&buf) {
        Ok(val) => Ok(val),
        Err(err) => Err(err),
    }
}

/// Parse json-rpc reply (defined by spec) and return user data embedded in
/// the reply.
fn parse_reply<T>(reply_raw: &[u8]) -> Result<T, Error>
where
    T: serde::de::DeserializeOwned,
{
    trace!("JSON response: {}", String::from_utf8_lossy(reply_raw));

    match serde_json::from_slice::<Response>(reply_raw) {
        Ok(reply) => {
            if let Some(vers) = reply.jsonrpc {
                if vers != "2.0" {
                    return Err(Error::InvalidVersion);
                }
            }
            if !reply.id.is_number() || reply.id.as_i64().unwrap() != 0 {
                return Err(Error::InvalidReplyId);
            }

            if let Some(err) = reply.error {
                Err(Error::RpcError {
                    code: match err.code {
                        -32700 => RpcCode::ParseError,
                        -32600 => RpcCode::InvalidRequest,
                        -32601 => RpcCode::MethodNotFound,
                        -32602 => RpcCode::InvalidParams,
                        -32603 => RpcCode::InternalError,
                        val => {
                            if val == -(Errno::ENOENT as i32) {
                                RpcCode::NotFound
                            } else if val == -(Errno::EEXIST as i32) {
                                RpcCode::AlreadyExists
                            } else {
                                error!("Unknown json-rpc error code {}", val);
                                RpcCode::InternalError
                            }
                        }
                    },
                    msg: err.message,
                })
            } else {
                match reply.result {
                    Some(result) => match serde_json::from_value::<T>(result) {
                        Ok(val) => Ok(val),
                        Err(err) => Err(Error::ParseError(err)),
                    },
                    // if there is no result fabricate null value == ()
                    None => match serde_json::from_value::<T>(
                        serde_json::value::Value::Null,
                    ) {
                        Ok(val) => Ok(val),
                        Err(err) => Err(Error::ParseError(err)),
                    },
                }
            }
        }
        Err(err) => Err(Error::ParseError(err)),
    }
}
