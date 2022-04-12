//! jsonrpc module helps with implementing jsonrpc handlers on the server
//! side. It registers handler with spdk and provides simple (de)serialization
//! of arguments using serde rust library. It makes use of async/await futures
//! for executing async code in jsonrpc handlers.

use std::{
    boxed::Box,
    ffi::{c_void, CStr, CString},
    fmt,
    os::raw::c_char,
    pin::Pin,
};

use futures::future::Future;
use nix::errno::Errno;
use serde::{Deserialize, Serialize};

use spdk_rs::libspdk::{
    spdk_json_val,
    spdk_json_write_val_raw,
    spdk_jsonrpc_begin_result,
    spdk_jsonrpc_end_result,
    spdk_jsonrpc_request,
    spdk_jsonrpc_send_error_response,
    spdk_rpc_register_method,
    SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
    SPDK_JSONRPC_ERROR_INVALID_PARAMS,
    SPDK_JSONRPC_ERROR_INVALID_REQUEST,
    SPDK_JSONRPC_ERROR_METHOD_NOT_FOUND,
    SPDK_JSONRPC_ERROR_PARSE_ERROR,
    SPDK_JSON_VAL_OBJECT_BEGIN,
    SPDK_RPC_RUNTIME,
};

use crate::core::Reactors;

/// Possible json-rpc return codes from method handlers
#[derive(Debug, Clone, Copy)]
pub enum Code {
    ParseError,
    InvalidRequest,
    MethodNotFound,
    InvalidParams,
    InternalError,
    NotFound,
    AlreadyExists,
}

impl From<Code> for i32 {
    fn from(code: Code) -> i32 {
        match code {
            Code::ParseError => SPDK_JSONRPC_ERROR_PARSE_ERROR,
            Code::InvalidRequest => SPDK_JSONRPC_ERROR_INVALID_REQUEST,
            Code::MethodNotFound => SPDK_JSONRPC_ERROR_METHOD_NOT_FOUND,
            Code::InvalidParams => SPDK_JSONRPC_ERROR_INVALID_PARAMS,
            Code::InternalError => SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
            Code::NotFound => -(Errno::ENOENT as i32),
            Code::AlreadyExists => -(Errno::EEXIST as i32),
        }
    }
}

/// A trait required to be implemented by error objects returned from
/// RPC handlers.
pub trait RpcErrorCode {
    /// Get RPC error code which should be used for the error returned back
    /// to the RPC client.
    fn rpc_error_code(&self) -> Code;
}

/// Error object returned from json-rpc method handlers
pub struct JsonRpcError {
    pub code: Code,
    pub message: String,
}

impl JsonRpcError {
    /// Create json-rpc error object
    pub fn new<T>(code: Code, message: T) -> Self
    where
        T: ToString,
    {
        Self {
            code,
            message: message.to_string(),
        }
    }

    /// Map json-rpc error code to integer defined in json-rpc spec.
    /// Then we have server implementation specific error codes which in case
    /// of SPDK are inverted errno values so for example ENOENT becomes
    /// -ENOENT jsonrpc error code.
    pub fn json_code(&self) -> i32 {
        self.code.into()
    }
}

impl RpcErrorCode for JsonRpcError {
    fn rpc_error_code(&self) -> Code {
        self.code
    }
}

impl fmt::Display for JsonRpcError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl fmt::Debug for JsonRpcError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for JsonRpcError {}

pub type Result<T, E = JsonRpcError> = std::result::Result<T, E>;

pub fn print_error_chain(err: &dyn std::error::Error) -> String {
    let mut msg = format!("{}", err);
    let mut opt_source = err.source();
    while let Some(source) = opt_source {
        msg = format!("{}: {}", msg, source);
        opt_source = source.source();
    }
    msg
}

/// Extract JSON object from text, trim any pending characters which follow
/// the closing bracket of the object.
fn extract_json_object(
    params: &spdk_json_val,
) -> std::result::Result<String, String> {
    if params.type_ != SPDK_JSON_VAL_OBJECT_BEGIN {
        return Err("JSON parameters must be an object".to_owned());
    }
    let text = unsafe {
        CStr::from_ptr(params.start as *const c_char)
            .to_str()
            .unwrap()
    };
    // find corresponding '}' for the object
    let mut level = 0;
    for (i, c) in text.chars().enumerate() {
        if c == '{' {
            level += 1;
        } else if c == '}' {
            level -= 1;
            if level == 0 {
                return Ok(text[0 ..= i].to_string());
            }
        }
    }
    // should not happen as params are validated by spdk before passed to us
    Err("JSON parameters object not properly terminated".to_owned())
}

/// A generic handler called by spdk for handling incoming jsonrpc call. The
/// task of this handler is to parse input parameters, invoke user-supplied
/// handler, and encode output parameters.
unsafe extern "C" fn jsonrpc_handler<H, P, R, E>(
    request: *mut spdk_jsonrpc_request,
    params: *const spdk_json_val,
    arg: *mut c_void,
) where
    H: 'static
        + Fn(P) -> Pin<Box<dyn Future<Output = std::result::Result<R, E>>>>,
    P: 'static + for<'de> Deserialize<'de>,
    R: Serialize,
    E: RpcErrorCode + std::error::Error,
{
    let handler: Box<H> = Box::from_raw(arg as *mut H);

    // get json parameters in form of a string
    let params = if params.is_null() {
        "null".to_owned()
    } else {
        match extract_json_object(&*params) {
            Ok(val) => val,
            Err(msg) => {
                let msg = CString::new(msg).unwrap();
                spdk_jsonrpc_send_error_response(
                    request,
                    SPDK_JSONRPC_ERROR_INVALID_PARAMS,
                    msg.as_ptr(),
                );
                return;
            }
        }
    };
    // deserialize parameters
    match serde_json::from_str::<P>(&params) {
        Ok(args) => {
            let fut = async move {
                // call user provided handler for the method
                let res = handler(args).await;
                // encode return value
                match res {
                    Ok(val) => {
                        // print json-rpc header
                        let w_ctx = spdk_jsonrpc_begin_result(request);
                        if w_ctx.is_null() {
                            // request is freed in begin_result if it fails
                            error!("Failed to write json-rpc message header");
                            return;
                        }
                        // serialize result to string
                        let data =
                            CString::new(serde_json::to_string(&val).unwrap())
                                .unwrap();
                        spdk_json_write_val_raw(
                            w_ctx,
                            data.as_ptr() as *const c_void,
                            data.as_bytes().len() as u64,
                        );
                        spdk_jsonrpc_end_result(request, w_ctx);
                    }
                    Err(err) => {
                        let code = err.rpc_error_code();
                        let msg = print_error_chain(&err);
                        error!("{}", msg);
                        let cerr = CString::new(msg).unwrap();
                        spdk_jsonrpc_send_error_response(
                            request,
                            code.into(),
                            cerr.as_ptr(),
                        );
                    }
                }
            };

            // it is expected rpc runs on the first core
            Reactors::master().send_future(fut);
        }
        Err(err) => {
            // parameters are not what is expected
            let msg = CString::new(err.to_string()).unwrap();
            spdk_jsonrpc_send_error_response(
                request,
                SPDK_JSONRPC_ERROR_INVALID_PARAMS,
                msg.as_ptr(),
            );
        }
    }
}

/// Register new json-rpc method with given name and handler having form of
/// a closure returning a future.
///
/// We use serde library with serialize/deserialize macro on structs which
/// represent arguments and return values to allow us to define new
/// json-rpc methods in rust with minimum code.
///
/// # Example:
/// ```ignore
/// use futures::future::Future;
/// use serde::{Deserialize, Serialize};
/// use io_engine::jsonrpc::{jsonrpc_register, Result};
/// use std::pin::Pin;
/// use futures::{future, FutureExt};
/// use futures_util::future::FutureExt;
///
/// #[derive(Deserialize)]
/// struct Args {
///     name: String,
/// }
///
/// #[derive(Serialize)]
/// struct Reply {
///     result: String,
/// }
///
/// pub fn init() {
///     jsonrpc_register(
///         "hello",
///         |args: Args| -> Pin<Box<dyn Future<Output = Result<Reply>>>> {
///             future::ok(Reply {
///                 result: format!("Hello {}!", args.name),
///             })
///             .boxed_local()
///         },
///     );
/// }
/// ```
pub fn jsonrpc_register<P, H, R, E>(name: &str, handler: H)
where
    H: 'static + Fn(P) -> Pin<Box<dyn Future<Output = Result<R, E>>>>,
    P: 'static + for<'de> Deserialize<'de>,
    R: Serialize,
    E: RpcErrorCode + std::error::Error,
{
    let name = CString::new(name).unwrap();
    let handler_ptr = Box::into_raw(Box::new(handler)) as *mut c_void;

    unsafe {
        spdk_rpc_register_method(
            name.as_ptr(),
            Some(jsonrpc_handler::<H, P, R, E>),
            handler_ptr,
            SPDK_RPC_RUNTIME,
        );
    }
}
