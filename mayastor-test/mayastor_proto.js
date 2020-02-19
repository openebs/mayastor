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

  //FIXME: the correct way to do this is to enumerate all the members,
  // and create the map from that. This will do for now.
  return {
    ShareProtocolReplica: {
      REPLICA_NONE: pkgDef.mayastor.ShareProtocolReplica.type.value.find(
        ent => ent.name == 'REPLICA_NONE'
      ).number,
      REPLICA_NVMF: pkgDef.mayastor.ShareProtocolReplica.type.value.find(
        ent => ent.name == 'REPLICA_NVMF'
      ).number,
      REPLICA_ISCSI: pkgDef.mayastor.ShareProtocolReplica.type.value.find(
        ent => ent.name == 'REPLICA_ISCSI'
      ).number,
    },
    ShareProtocolNexus: {
      NEXUS_NBD: pkgDef.mayastor.ShareProtocolNexus.type.value.find(
        ent => ent.name == 'NEXUS_NBD'
      ).number,
      NEXUS_NVMF: pkgDef.mayastor.ShareProtocolNexus.type.value.find(
        ent => ent.name == 'NEXUS_NVMF'
      ).number,
      NEXUS_ISCSI: pkgDef.mayastor.ShareProtocolNexus.type.value.find(
        ent => ent.name == 'NEXUS_ISCSI'
      ).number,
    },
  };
}

module.exports = {
  getConstants,
};
