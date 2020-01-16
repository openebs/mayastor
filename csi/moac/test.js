// Unit tests for the moac components

const assert = require('chai').assert;
const logger = require('./logger');
const watcherTest = require('./watcher_test.js');
const grpcTest = require('./grpc_client_test.js');
const nodesTest = require('./nodes_test.js');
const poolsTest = require('./pools_test.js');
const volumesTest = require('./volumes_test.js');
const commanderTest = require('./commander_test.js');
const csiTest = require('./csi_test.js');
const restApiServer = require('./rest_api_test.js');

logger.setLevel('debug');

describe('moac', function() {
  describe('watcher', watcherTest);
  describe('grpc client', grpcTest);
  describe('node operator', nodesTest);
  describe('pool operator', poolsTest);
  describe('volume operator', volumesTest);
  describe('commander operator', commanderTest);
  describe('CSI controller', csiTest);
  describe('REST API server', restApiServer);
});
