// Miscellaneous objects and funcs shared between moac modules.

'use strict';

const grpc = require('grpc-uds');
const { GrpcClient, GrpcError, GrpcHandle } = require('./grpc_client');

const PLUGIN_NAME = 'io.openebs.csi-mayastor';

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

module.exports = {
  PLUGIN_NAME,
  parseMayastorNodeId,
};
