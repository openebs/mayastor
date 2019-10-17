// Miscellaneous objects and funcs shared between moac modules.

'use strict';

const protoLoader = require('@grpc/proto-loader');
const grpc = require('grpc-uds');

const PLUGIN_NAME = 'io.openebs.csi-mayastor';
const PROTO_PATH = __dirname + '/proto/mayastor_service.proto';

// Load mayastor proto file with mayastor service
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: false,
  longs: String,
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

// Parse mayastor node ID in form "mayastor://node-name/host:port" and
// return node name and endpoint.
function parseMayastorNodeId(nodeId) {
  let parts = nodeId.split('/');

  if (
    parts.length != 4 ||
    parts[0] !== 'mayastor:' ||
    parts[1] !== '' ||
    !parts[2] ||
    !parts[3]
  ) {
    throw new GrpcError(
      grpc.status.INVALID_ARGUMENT,
      'Invalid mayastor node ID: ' + nodeId
    );
  }
  return {
    node: parts[2],
    endpoint: parts[3],
  };
}

// Return true if the storage pool is accessible via gRPC
function isPoolAccessible(pool) {
  return pool.state == 'ONLINE' || pool.state == 'DEGRADED';
}

module.exports = {
  PLUGIN_NAME,
  isPoolAccessible,
  mayastor,
  GrpcError,
  parseMayastorNodeId,
};
