//! json-rpc error enum which contains all different errors which can happen
//! when sending request and processing reply from json-rpc server.

use std::{convert::From, fmt, io};
use tonic::{Code, Status};

#[derive(Debug, PartialEq)]
pub enum RpcCode {
    ParseError,
    InvalidRequest,
    MethodNotFound,
    InvalidParams,
    InternalError,
    NotFound,
    AlreadyExists,
}

#[derive(Debug)]
pub enum Error {
    InvalidVersion,
    InvalidReplyId,
    IoError(io::Error),
    ParseError(serde_json::Error),
    ConnectError { sock: String, err: io::Error },
    RpcError { code: RpcCode, msg: String },
    GenericError(String),
}

impl Error {
    /// Conversion from jsonrpc error to grpc status.
    ///
    /// NOTE: normally we would have a From<Error> trait for Status type, but
    /// we can't since both Status type and From trait are external.
    pub fn into_status(self) -> Status {
        match self {
            Error::RpcError {
                code,
                msg,
            } => {
                let code = match code {
                    RpcCode::InvalidParams => Code::InvalidArgument,
                    RpcCode::NotFound => Code::NotFound,
                    RpcCode::AlreadyExists => Code::AlreadyExists,
                    _ => Code::Internal,
                };
                Status::new(code, msg)
            }
            _ => Status::new(Code::Internal, self.to_string()),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::InvalidVersion => write!(f, "Invalid json-rpc version"),
            Error::InvalidReplyId => write!(f, "Invalid ID of json-rpc reply"),
            Error::ConnectError {
                sock,
                err,
            } => write!(f, "Error connecting to {}: {}", sock, err),
            Error::IoError(err) => write!(f, "IO error: {}", err),
            Error::ParseError(err) => write!(f, "Invalid json reply: {}", err),
            Error::RpcError {
                code,
                msg,
            } => write!(f, "Json-rpc error {:?}: {}", code, msg),
            Error::GenericError(msg) => write!(f, "{}", msg),
        }
    }
}

impl From<Error> for Status {
    fn from(e: Error) -> Self {
        e.into_status()
    }
}
// Automatic conversion functions for simply using .into() on various return
// types follow

impl std::error::Error for Error {
    fn cause(&self) -> Option<&dyn std::error::Error> {
        None
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::IoError(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::ParseError(err)
    }
}

impl From<&str> for Error {
    fn from(err: &str) -> Self {
        Error::GenericError(err.to_owned())
    }
}

impl From<String> for Error {
    fn from(err: String) -> Self {
        Error::GenericError(err)
    }
}
