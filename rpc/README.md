# RPC

This crate provides common data structures between `JSON-RPC` and `gRPC`.

## Message and service definitions for Mayastor (.proto)

The proto files initially might seem somewhat unusual. The service and the messages
are split up in to two separate files. The reason for this is to avoid the need
to include [tower-grpc](http://github.com/tower-grpc), futures and the likes when using the generated .rs files.

We re-use the generated .rs files at different locations to avoid duplicating structs
between the JSON and gRPC server. This allows us to write code such as:

```rust
pub fn function(&mut self, request: Request<T>) -> Self::FutureReponseType {
    let msg = request.into_inner();
    let fut = jsonrpc::call(&self.socket, "function", Some(msg))
        .map_err(|e| e.into_satus())
        .map(Resonse::new);
    Box::new(fut)
}
```

In the above example, conversion from gRPC, RUST, to JSON is all handled transparently.

### Adding methods and or messages

When adding a method or new message type, an implementation must be provided in
[mayastor_svc.rs](../../csi/src/mayastor_svc.rs).

### Future work

 - Directly integrate tower-grpc in mayastor itself avoiding the need for the translation
in the first place.
 - Integrate all used RPC methods natively in RUST getting rid of `rpc::jsonrpc`


