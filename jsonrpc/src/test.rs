//! Unit tests for json-rpc client module

use self::error::Error;
use super::*;
use futures::Stream;
use nix::errno::Errno;
use serde_json::json;
use std::{fs, panic, path::Path, thread};
use tokio::{
    io::{read_to_end, write_all},
    net::UnixListener,
    runtime::current_thread::Runtime,
};

/// Socket path to the test json-rpc server
const SOCK_PATH: &str = "/tmp/jsonrpc-ut.sock";

/// Needed because jsonrpc error contains nested boxed std::error::Error trait.
impl panic::UnwindSafe for Error {}

#[derive(Debug, Serialize, Deserialize)]
struct EmptyArgs {}

/// The main test work horse. It runs a setup before the unit test and tear-down
/// after the unit test. The setup involves starting a unix domain socket
/// server. It is customizable by providing two closures:
///
///  1) handler for constructing reply from the server and
///  2) test callback evaluating a return value from the json-rpc client call
///
/// Beware that rust executes the tests in parallel so whatever is done in this
/// function must preserve independance of the tests on each other.
fn run_test<A, R, H, T>(method: &str, arg: A, handler: H, test: T)
where
    A: serde::ser::Serialize,
    R: 'static + serde::de::DeserializeOwned + panic::UnwindSafe + Send,
    H: FnOnce(Request) -> Vec<u8> + 'static,
    T: FnOnce(Result<R, Error>) -> () + panic::UnwindSafe,
{
    let sock = format!("{}.{:?}", SOCK_PATH, thread::current().id());
    let sock_path = Path::new(&sock);
    // Cleanup should be called at all places where we exit from this function
    let cleanup = || {
        let _ = fs::remove_file(&sock_path);
    };
    let server = match UnixListener::bind(&sock_path) {
        Ok(server) => server,
        Err(_) => {
            // most likely the socket file exists, remove it and retry
            cleanup();
            UnixListener::bind(&sock_path).unwrap()
        }
    };
    // create tokio futures runtime
    let mut rt = Runtime::new().unwrap();

    // define handling of client requests at the server side
    rt.spawn({
        // stream of incoming connections from clients
        server
            .incoming()
            .into_future()
            .map_err(|(err, stream)| {
                drop(stream);
                err
            })
            // read client request
            .and_then(move |(sock, stream)| {
                drop(stream);
                read_to_end(sock.unwrap(), Vec::new())
            })
            // create reply for the request and send it
            .and_then(move |(sock, buf)| {
                let req: Request = serde_json::from_slice(&buf).unwrap();
                let resp = handler(req);
                write_all(sock, resp)
            })
            // wait for the reply to be sent
            .and_then(move |(_sock, _buf)| Ok(()))
            .map_err(|e| panic!("err={:?}", e))
    });

    // write to the server using our jsonrpc client
    let call_res = rt.block_on(call(&sock, method, Some(arg)));
    rt.run().unwrap();

    // test if the return value from client call is ok
    let res = panic::catch_unwind(move || test(call_res));
    cleanup();
    assert!(res.is_ok());
}

#[test]
fn normal_request_reply() {
    #[derive(Debug, Serialize, Deserialize)]
    struct Args {
        msg: String,
        code: i32,
        flag: bool,
    }

    let args = Args {
        msg: "some message".to_owned(),
        code: -123,
        flag: true,
    };

    run_test(
        "invert_method",
        args,
        // we invert int and bool values in the request and send it back
        |req| {
            assert_eq!(req.method, "invert_method");
            assert_eq!(req.id.as_i64().unwrap(), 0);
            assert_eq!(req.jsonrpc.unwrap(), "2.0");

            let params: Args =
                serde_json::from_value(req.params.unwrap()).unwrap();

            let resp = Response {
                error: None,
                id: req.id,
                jsonrpc: Some("2.0".to_owned()),
                result: Some(json!({
                    "msg": params.msg.clone(),
                    "code": -params.code,
                    "flag": !params.flag,
                })),
            };

            serde_json::to_vec_pretty(&resp).unwrap()
        },
        |res: Result<Args, Error>| match res {
            Ok(res) => {
                assert_eq!(&res.msg, "some message");
                assert_eq!(res.code, 123);
                assert!(!res.flag);
            }
            Err(err) => panic!(format!("{}", err)),
        },
    );
}

#[test]
fn invalid_json() {
    run_test(
        "method",
        EmptyArgs {},
        |_req| {
            // missing quotes on result key below
            r#"{
            "id": 0,
            "jsonrpc": "2.0",
            result: {},
            "#
            .to_string()
            .into_bytes()
        },
        |res: Result<(), Error>| match res {
            Ok(_) => panic!("Expected error and got ok"),
            Err(Error::ParseError(_)) => (),
            Err(err) => panic!(format!("Wrong error type: {}", err)),
        },
    );
}

#[test]
fn connect_error() {
    // create tokio futures runtime
    let mut rt = Runtime::new().unwrap();
    // try to connect to server which does not exist
    let call_res: Result<(), Error> =
        rt.block_on(call("/crazy/path/look", "method", Some(())));
    rt.run().unwrap();
    match call_res {
        Ok(_) => panic!("Expected error and got ok"),
        Err(Error::ConnectError {
            sock: _sock,
            err: _err,
        }) => (),
        Err(err) => panic!(format!("Wrong error type: {}", err)),
    }
}

#[test]
fn invalid_version() {
    run_test(
        "method",
        EmptyArgs {},
        |req| {
            let resp = Response {
                error: None,
                id: req.id,
                jsonrpc: Some("1.0".to_owned()),
                result: None,
            };

            serde_json::to_vec_pretty(&resp).unwrap()
        },
        |res: Result<(), Error>| match res {
            Ok(_) => panic!("Expected error and got ok"),
            Err(Error::InvalidVersion) => (),
            Err(err) => panic!(format!("Wrong error type: {}", err)),
        },
    );
}

#[test]
fn missing_version() {
    run_test(
        "method",
        EmptyArgs {},
        |req| {
            let resp = Response {
                error: None,
                id: req.id,
                jsonrpc: None,
                result: Some(json!("hello this is result")),
            };

            serde_json::to_vec_pretty(&resp).unwrap()
        },
        |res: Result<String, Error>| match res {
            Ok(_) => (),
            Err(err) => panic!(format!("{}", err)),
        },
    );
}

#[test]
fn wrong_reply_id() {
    run_test(
        "method",
        EmptyArgs {},
        |_req| {
            let resp = Response {
                error: None,
                id: json!("12"),
                jsonrpc: Some("2.0".to_owned()),
                result: Some(json!("hello this is result")),
            };

            serde_json::to_vec_pretty(&resp).unwrap()
        },
        |res: Result<String, Error>| match res {
            Ok(_) => panic!("Expected error and got ok"),
            Err(Error::InvalidReplyId) => (),
            Err(err) => panic!(format!("Wrong error type: {}", err)),
        },
    );
}

#[test]
fn empty_result_unexpected() {
    run_test(
        "method",
        EmptyArgs {},
        |req| {
            let resp = Response {
                error: None,
                id: req.id,
                jsonrpc: Some("2.0".to_owned()),
                result: Some(json!("unexpected value")),
            };

            serde_json::to_vec_pretty(&resp).unwrap()
        },
        |res: Result<(), Error>| match res {
            Ok(_) => panic!("Expected error and got ok"),
            Err(Error::ParseError(_)) => (),
            Err(err) => panic!(format!("Wrong error type: {}", err)),
        },
    );
}

#[test]
fn empty_result_expected() {
    run_test(
        "method",
        EmptyArgs {},
        |req| {
            let resp = Response {
                error: None,
                id: req.id,
                jsonrpc: Some("2.0".to_owned()),
                result: None,
            };

            serde_json::to_vec_pretty(&resp).unwrap()
        },
        |res: Result<(), Error>| match res {
            Ok(_) => (),
            Err(err) => panic!(format!("Unexpected error {}", err)),
        },
    );
}

#[test]
fn rpc_error() {
    run_test(
        "method",
        EmptyArgs {},
        |req| {
            let resp = Response {
                error: Some(RpcError {
                    code: -(Errno::ENOENT as i32),
                    message: "Not found".to_owned(),
                    data: None,
                }),
                id: req.id,
                jsonrpc: Some("2.0".to_owned()),
                result: None,
            };

            serde_json::to_vec_pretty(&resp).unwrap()
        },
        |res: Result<(), Error>| match res {
            Ok(_) => panic!("Expected error and got ok"),
            Err(Error::RpcError {
                code,
                msg,
            }) => {
                assert_eq!(code, RpcCode::NotFound);
                assert_eq!(&msg, "Not found");
            }
            Err(err) => panic!(format!("Wrong error type: {}", err)),
        },
    );
}
