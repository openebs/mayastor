'use strict';

const path = require('path');
const protoLoader = require('@grpc/proto-loader');
const grpc = require('grpc');

const constants = {};

const defs = Object.values(
  grpc.loadPackageDefinition(
    protoLoader.loadSync(
      path.join(__dirname, '..', '..', 'rpc', 'mayastor-api', 'protobuf', 'mayastor.proto'),
      {
        // this is to load google/descriptor.proto
        includeDirs: ['./node_modules/protobufjs']
      }
    )
  ).mayastor
);

defs.forEach((ent) => {
  if (ent.format && ent.format.indexOf('EnumDescriptorProto') >= 0) {
    ent.type.value.forEach((variant) => {
      constants[variant.name] = variant.number;
    });
  }
});

module.exports = constants;
