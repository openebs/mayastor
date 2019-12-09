# Errors in MayaStor

This doc describes representation of errors, which happen in MayaStor and
are expected. Unexpected errors end up with a crash or panic message with
a stack trace. The expected ones are represented by an object which bubbles
up through the layers of the code up to the RPC method handler, where the
error is serialized and sent to the client.

## Background

Originally we started representing errors by `JsonRpcError` object with
`code` and `message` fields. That worked quite well, but had a major
limitation.

Constructing error stacks is cumbersome. An error which originates deep
in the code far from RPC method entry point needs to be augmented by
a context information, which is not available when the error is created.
Thus adding more context to an error from lower layer should be easy
and it was not.

## Snafu crate

Snafu is one of many libraries in rust for representing failures. We picked
snafu because it seems popular in rust, it supports elegant stacking of errors
and the definition of errors and error messages makes use of preprocessor and
is easy.

Example of error definition follows. We introduce a new enum type with three
variants. These are high-level errors for replica RPC methods. `source` field
is a link to lower level error describing the problem in more detail.

```rust
#[derive(Debug, Snafu)]
pub enum RpcError {
    #[snafu(display("Failed to create replica {}", uuid))]
    CreateReplica { source: Error, uuid: String },
    #[snafu(display("Failed to destroy replica {}", uuid))]
    DestroyReplica { source: Error, uuid: String },
    #[snafu(display("Failed to (un)share replica {}", uuid))]
    ShareReplica { source: Error, uuid: String },
}
```

## SPDK errors

MayaStor is essentially a wrapper around SPDK. Many errors originate in SPDK
functions/callbacks which return some kind of boolean status (success/failure)
or errno value. We want to make handling such errors straightforward. As soon
as the errno gets to the rust code, an `nix::errno::Errno` object should be
created for it. The object will take care of translating errno i32 value to
a human readable text. Such errors are never returned directly from RPC methods.
Instead they must be wrapped by snafu error, which provides more context than
just meaning of the errno. There are utility functions for returning errno
results from async callbacks to avoid code duplication.

## Design of the new errors

1. We want the errors to be decentralized. Instead of a giant list of all
   potential mayastor errors we want them to be defined where they are used.
   The error definition can be per file or per group of closely related files.

2. Names of the errors should be short, clear and consistent across the whole
   mayastor code. If the error is a high level error specifying context of the
   error, its name should be `Operation``Object` (i.e. `CreateReplica`). If
   the error describes the root cause of a problem, then the name should be
   `Object``Problem` (i.e. `PoolNotFound`).

3. Take care to assign the right context data to the right layer or errors.
   For example if we have a high level `CreateReplica` error, that's where
   `uuid` of the replica should be mentioned. There might be 10 reasons why
   the create operation can fail and those reasons (=errors) can contain more
   detailed data. However those 10 errors don't need to contain uuid, because
   that information is added by the `CreateReplica` context.

4. Errors returned by RPC methods must implement `RpcErrorCode` trait. This
   is a specific requirement to mayastor because the RPC handler must know
   which RPC error code to associate with the error. Low level errors which
   aren't directly passed to RPC handlers don't need to implement the trait.

## Example implementation

Consider a model situation of adding new RPC methods for implementing
replica RPC methods: create, destroy, share. We start with these high level
errors:

```rust
/// These are high-level context errors one for each rpc method.
#[derive(Debug, Snafu)]
pub enum RpcError {
    #[snafu(display("Failed to create replica {}", uuid))]
    CreateReplica { source: Error, uuid: String },
    #[snafu(display("Failed to destroy replica {}", uuid))]
    DestroyReplica { source: Error, uuid: String },
    #[snafu(display("Failed to (un)share replica {}", uuid))]
    ShareReplica { source: Error, uuid: String },
}
```

It has to implement `RpcErrorCode` trait because the error is used in RPC
handlers. The trait impl is simple because we dive deeper along the error
chain to find out the appropriate RPC code.

```rust
impl RpcErrorCode for RpcError {
    fn rpc_error_code(&self) -> Code {
        match self {
            RpcError::CreateReplica { source, .. } => source.rpc_error_code(),
            RpcError::DestroyReplica { source, .. } => source.rpc_error_code(),
            RpcError::ShareReplica { source, .. } => source.rpc_error_code(),
        }
    }
}
```

The second layer of errors are the errors used in Replica object methods.
Some of them are terminal errors and some go even to deeper layer of code.

```rust
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("The pool \"{}\" does not exist", pool))]
    PoolNotFound { pool: String },
    #[snafu(display("Replica already exists"))]
    ReplicaExists { },
    #[snafu(display("Invalid parameters"))]
    InvalidParams { },
    #[snafu(display("Failed to create lvol"))]
    CreateLvol { source: Errno },
    #[snafu(display("Failed to destroy lvol"))]
    DestroyLvol { source: Errno },
    #[snafu(display("Replica has been already shared"))]
    ReplicaShared { },
    #[snafu(display("share nvmf"))]
    ShareNvmf { source: nvmf_target::Error },
    #[snafu(display("share iscsi"))]
    ShareIscsi { source: iscsi_target::Error },
    #[snafu(display("unshare nvmf"))]
    UnshareNvmf { source: nvmf_target::Error },
    #[snafu(display("unshare iscsi"))]
    UnshareIscsi { source: iscsi_target::Error },
    #[snafu(display("Invalid share protocol {} in request", protocol))]
    InvalidProtocol { protocol: i32 },
    #[snafu(display("Replica does not exist"))]
    ReplicaNotFound { },
}

impl RpcErrorCode for Error {
    fn rpc_error_code(&self) -> Code {
        match self {
            Error::PoolNotFound { .. } => Code::NotFound,
            Error::ReplicaNotFound { .. } => Code::NotFound,
            Error::ReplicaExists { .. } => Code::AlreadyExists,
            Error::InvalidParams { .. } => Code::InvalidParams,
            Error::CreateLvol { .. } => Code::InvalidParams,
            Error::InvalidProtocol { .. } => Code::InvalidParams,
            Error::ShareNvmf { source, .. } => source.rpc_error_code(),
            Error::ShareIscsi { source, .. } => source.rpc_error_code(),
            Error::UnshareNvmf { source, .. } => source.rpc_error_code(),
            Error::UnshareIscsi { source, .. } => source.rpc_error_code(),
            _ => Code::InternalError,
        }
    }
}
```

Sharing of replica is implemented either by nvmf target or iscsi target module.
Each module comes with its own error type. For brevity we mention only nvmf
target errors and not all of them:

```rust
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create nvmf target {}:{}", addr, port))]
    CreateTarget { addr: String, port: u16 },
    #[snafu(display(
        "Failed to destroy nvmf target {}: {}",
        endpoint,
        source
    ))]
    DestroyTarget { source: Errno, endpoint: String },
    #[snafu(display("Invalid nvmf target address \"{}\"", addr))]
    TargetAddress { addr: String },
    #[snafu(display("Failed to init opts for nvmf tcp transport"))]
    InitOpts {},
    #[snafu(display("Failed to create nvmf tcp transport"))]
    TcpTransport {},
    #[snafu(display("Failed to add nvmf tcp transport: {}", source))]
    AddTransport { source: Errno },
    #[snafu(display("nvmf target listen failed: {}", source))]
    ListenTarget { source: Errno },
    #[snafu(display("Failed to create a poll group"))]
    CreatePollGroup {},
    // ...
}

impl RpcErrorCode for Error {
    fn rpc_error_code(&self) -> Code {
        match self {
            Error::TargetAddress {
                ..
            } => Code::InvalidParams,
            _ => Code::InternalError,
        }
    }
}
```

We have seen how the tree of errors from least specific context errors to
most specific root cause errors is implemented. Now let's see it in action.
What is behind the following log file:

```
jsonrpc.rs: 218:: *ERROR*: Failed to create replica dbe4d7eb-118a-4d15-b789-a18d9af6ff21: Replica already exists
```

A stack of composable errors:
1. `Failed to create replica dbe4d7eb-118a-4d15-b789-a18d9af6ff21`
2. `Replica already exists`
