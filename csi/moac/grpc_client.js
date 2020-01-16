// gRPC client related utilities

'use strict';

const assert = require('assert');
const grpc = require('grpc-uds');
const grpc_promise = require('grpc-promise');
const protoLoader = require('@grpc/proto-loader');
const log = require('./logger').Logger('grpc');

const PROTO_PATH = __dirname + '/proto/mayastor_service.proto';

// Load mayastor proto file with mayastor service
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: false,
  longs: Number,
  enums: String,
  defaults: true,
  oneofs: true,
});
const mayastor = grpc.loadPackageDefinition(packageDefinition).mayastor_service;

// Grpc error object.
//
// List of grpc status codes:
//   OK: 0,
//   CANCELLED: 1,
//   UNKNOWN: 2,
//   INVALID_ARGUMENT: 3,
//   DEADLINE_EXCEEDED: 4,
//   NOT_FOUND: 5,
//   ALREADY_EXISTS: 6,
//   PERMISSION_DENIED: 7,
//   RESOURCE_EXHAUSTED: 8,
//   FAILED_PRECONDITION: 9,
//   ABORTED: 10,
//   OUT_OF_RANGE: 11,
//   UNIMPLEMENTED: 12,
//   INTERNAL: 13,
//   UNAVAILABLE: 14,
//   DATA_LOSS: 15,
//   UNAUTHENTICATED: 16
//
class GrpcError extends Error {
  constructor(code, msg) {
    if (msg === undefined) {
      msg = code;
      code = grpc.status.UNKNOWN;
    }
    super(msg);
    this.code = code;
  }
}

// Implementation of gRPC client encapsulating common code for calling a grpc
// method on a storage node (the node running mayastor).
class GrpcClient {
  constructor(nodeOperator) {
    this.nodes = nodeOperator;
  }

  // Create promise-friendly grpc client handle.
  _createClient(endpoint) {
    let client = new mayastor.Mayastor(
      endpoint,
      grpc.credentials.createInsecure()
    );
    grpc_promise.promisifyAll(client);
    return client;
  }

  // Get grpc mayastor service client for particular storage node.
  // Return null if there is not a node with such a name.
  _getNodeClient(nodeName) {
    let node = this.nodes.get(nodeName);

    if (!node) {
      return null;
    }
    return this._createClient(node.endpoint);
  }

  // Create grpc client handle suitable for calling grpc methods and pass it to
  // the callback function. Release the client handle when the callback is over.
  // The release of handle is exactly the reason why we need to use callback
  // style method. Unfortunately there are no destructors in JS.
  //
  // Throws internal grpc error if the client handle cannot be created (i.e.
  // because the node does not exist).
  //
  // @param nodeName   Name of the node to call gRPC method on.
  // @param callback   Callback called with the client handle argument. It
  //                   should be either sync or async fn returning a promise.
  // @returns  It returns whatever value is returned by the callback supplied by the user.
  //
  async with_handle(nodeName, callback) {
    let client = this._getNodeClient(nodeName);
    if (!client) {
      throw new GrpcError(
        grpc.status.INTERNAL,
        `MayaStor on node "${nodeName}" is not running`
      );
    }
    try {
      return await callback(new GrpcHandle(client));
    } finally {
      client.close();
    }
  }
}

// Thin wrapper around grpc client handle providing more user friendly api for
// calling remote methods and capable of producing trace log messages with
// details about the call.
class GrpcHandle {
  constructor(client) {
    this.client = client;
  }

  // Call a grpc method with arguments.
  //
  // @param method   Name of the grpc method.
  // @param args     Arguments of the grpc method.
  // @returns        Return value of the grpc method.
  async call(method, args) {
    log.trace(
      `Calling grpc method ${method} with arguments: ` + JSON.stringify(args)
    );
    let ret = await this.client[method]().sendMessage(args);
    log.trace(`Grpc method ${method} returned: ` + JSON.stringify(ret));
    return ret;
  }
}

module.exports = {
  GrpcError,
  GrpcHandle,
  GrpcClient,
  mayastor,
};
