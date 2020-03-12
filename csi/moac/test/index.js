// Unit tests for the moac components

const path = require('path');
const { spawn } = require('child_process');

const logger = require('../logger');
const workqTest = require('./workq_test.js');
const grpcTest = require('./grpc_client_test.js');
const watcherTest = require('./watcher_test.js');
const nodeObject = require('./node_test.js');
const poolObject = require('./pool_test.js');
const replicaObject = require('./replica_test.js');
const nexusObject = require('./nexus_test.js');
const nodeOperator = require('./node_operator_test.js');
const registryTest = require('./registry_test.js');
const eventStream = require('./event_stream_test.js');
const poolOperator = require('./pool_operator_test.js');
const volumeObject = require('./volume_test.js');
const volumesTest = require('./volumes_test.js');
const volumeOperator = require('./volume_operator_test.js');
const restApi = require('./rest_api_test.js');
const csiTest = require('./csi_test.js');

logger.setLevel('debug');

describe('moac', function() {
  describe('workq', workqTest);
  describe('grpc client', grpcTest);
  describe('watcher', watcherTest);
  describe('node object', nodeObject);
  describe('pool object', poolObject);
  describe('replica object', replicaObject);
  describe('nexus object', nexusObject);
  describe('node operator', nodeOperator);
  describe('registry', registryTest);
  describe('event stream', eventStream);
  describe('pool operator', poolOperator);
  describe('volume object', volumeObject);
  describe('volumes', volumesTest);
  describe('volume operator', volumeOperator);
  describe('REST API', restApi);
  describe('CSI controller', csiTest);

  // Start moac without k8s just to test basic errors
  it('start moac process', done => {
    let child = spawn(path.join(__dirname, '..', 'index.js'), ['-s']);
    let stderr = '';

    child.stdout.on('data', data => {
      if (data.toString().indexOf('🚀') >= 0) {
        child.kill();
      }
    });
    child.stderr.on('data', data => {
      stderr += data.toString();
    });
    child.on('close', code => {
      if (code == 0) {
        done();
      } else {
        done(new Error());
      }
    });
  });
});
