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
  // Create promise-friendly grpc client handle.
  //
  // @param {string} endpoint   Host and port that mayastor server listens on.
  constructor(endpoint) {
    let handle = new mayastor.Mayastor(
      endpoint,
      grpc.credentials.createInsecure()
    );
    grpc_promise.promisifyAll(handle);
    this.handle = handle;
  }

  // Call a grpc method with arguments.
  //
  // @param {string} method   Name of the grpc method.
  // @param {object} args     Arguments of the grpc method.
  // @returns {*} Return value of the grpc method.
  async call(method, args) {
    log.trace(
      `Calling grpc method ${method} with arguments: ${JSON.stringify(args)}`
    );
    let ret = await this.handle[method]().sendMessage(args);
    log.trace(`Grpc method ${method} returned: ${JSON.stringify(ret)}`);
    return ret;
  }

  // Close the grpc handle. The client should not be used after that.
  close() {
    this.handle.close();
  }
}

module.exports = {
  GrpcClient,
  // for easy access to grpc codes
  GrpcCode: grpc.status,
  GrpcError,
  mayastor,
};
