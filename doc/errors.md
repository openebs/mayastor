# Errors in MayaStor

This document describes error handling within MayaStor.
Unhandled errors result in a crash/panic message with
a stack trace. The expected ones are represented by an object which bubbles
up through the layers of the code up to the RPC method handler, where the
error is serialized and sent back to the client.

## Background

Originally we started representing errors by `JsonRpcError` object with
`code` and `message` fields. That worked quite well, but had a major
limitation.

Constructing error stacks is cumbersome. An error which originates deep
in the code far from RPC method entry point needs to be augmented by
contextual information, which is not available when the error occurs.
Thus adding more context to an error from within lower layer should be easy,
but it was not.

## Snafu crate

Snafu is one of many libraries in rust for representing failures. We picked
snafu because:

1. It seems relatively popular in rust
2. It provides an elegant way of chaining errors and the definition of errors and error
messages makes use of a preprocessor and is easy.

An example of an error definition follows. We introduce a new enum type with three
variants. These are high-level errors for the replica RPC methods. The`source` field
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

MayaStor, essentially wraps around the errors originating {S,D} PDK.
Many errors originate in SPDK functions/callbacks which return some kind of boolean status
(success/failure) or an `errno` value. We want to make handling such errors straightforward.
As soon as the `errno` returns to rust a `nix::errno::Errno` object should be
created for it. This object will take care of mapping the `errno` (i32) value to
a human readable format. Such errors are never returned directly from RPC methods.
Instead, they must be wrapped by a snafu error, which provides more context than
just textual representation of the `errno`. There are utility functions for returning errno
results from async callbacks to avoid code duplication.

## Design of the new errors

1. We want the errors to be decentralized. Instead of a giant list of all
   potential mayastor errors we want them to be defined where they are used.
   The error definition can be per file or per group of closely related files.

2. Error names should be short, clear and consistent across the whole
   code base. If the error is a high level error specifying context of the
   error, its name should be `Operation` + `Object` (i.e. `CreateReplica`). If
   the error describes the root cause of a problem, then the name should be
   `Object` + `Problem` (i.e. `PoolNotFound`).

3. Take care to assign the right context data to the right layer or errors.
   For example if we have a high level `CreateReplica` error, that's where
   `uuid` of the replica should be mentioned. There may be 10 different reasons why
   the create operation failed, and those reasons (errors) may contain more
   detailed information. However, all those 10 errors don't need to contain uuid, because
   that information is already added by the `CreateReplica` context.

4. Errors returned by RPC methods must implement `RpcErrorCode` trait. This
   is a specific requirement to mayastor because the RPC handler must know
   which RPC error code to associate with the error. Low level errors which
   aren't directly passed to RPC handlers don't need to implement the trait.

## Example implementation

Consider the situation of adding new RPC methods for implementing
replica RPC methods: create, destroy and share. We start with these high level
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

It has to implement `RpcErrorCode` trait because the errors are used in the RPC
handlers. The impl trait is simple because we go deeper down the error
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

The second layer of errors are the errors used in in the Replica object methods.
Some of them are terminal errors, and some go even to deeper layers of code.

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

Sharing of replica is implemented either by the nvmf or iscsi target module.
Each module comes with its own error type. For brevity, we mention only nvmf
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

We have seen how the tree of errors from the least specific context errors to
most specific root cause errors implemented. Now let's see it in action.
What is behind the following log file:

```
jsonrpc.rs: 218:: *ERROR*: Failed to create replica dbe4d7eb-118a-4d15-b789-a18d9af6ff21: Replica already exists
```

A stack of composable errors:
1. `Failed to create replica dbe4d7eb-118a-4d15-b789-a18d9af6ff21`
2. `Replica already exists`
