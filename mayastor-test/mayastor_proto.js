'use strict';

const path = require('path');
const protoLoader = require('@grpc/proto-loader');
// we can't use grpc-kit because we need to connect to UDS and that's currently
// possible only with grpc-uds.
const grpc = require('grpc-uds');

function getConstants() {
  const pkgDef = grpc.loadPackageDefinition(
    protoLoader.loadSync(
      path.join(__dirname, '..', 'rpc', 'proto', 'mayastor.proto'),
      {
        // this is to load google/descriptor.proto
        includeDirs: ['./node_modules/protobufjs'],
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
      }
    )
  );

//FIXME: the correct way to do this is to enumerate all the members of ShareProtocol,
// ans create the map from that. This will do for now.
  return {
      ShareProtocol: {
          NONE: pkgDef.mayastor.ShareProtocol.type.value.find(ent => ent.name == 'NONE').number,
          NVMF: pkgDef.mayastor.ShareProtocol.type.value.find(ent => ent.name == 'NVMF').number,
          ISCSI: pkgDef.mayastor.ShareProtocol.type.value.find(ent => ent.name == 'ISCSI').number,
          NBD: pkgDef.mayastor.ShareProtocol.type.value.find(ent => ent.name == 'NBD').number,
      },
  };
}

module.exports = {
    getConstants,
};

