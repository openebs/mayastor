// Unit tests for the moac components

const path = require('path');
const { spawn } = require('child_process');
const chai = require('chai');
const dirtyChai = require('dirty-chai');

const logger = require('../logger');
const workqTest = require('./workq_test.js');
const grpcTest = require('./grpc_client_test.js');
const watcherTest = require('./watcher_test.js');
const nodeObject = require('./node_test.js');
const poolObject = require('./pool_test.js');
const replicaObject = require('./replica_test.js');
const nexusObject = require('./nexus_test.js');
const nodeOperator = require('./node_operator_test.js');
const natsTest = require('./nats_test.js');
const registryTest = require('./registry_test.js');
const eventStream = require('./event_stream_test.js');
const poolOperator = require('./pool_operator_test.js');
const volumeObject = require('./volume_test.js');
const volumesTest = require('./volumes_test.js');
const volumeOperator = require('./volume_operator_test.js');
const restApi = require('./rest_api_test.js');
const csiTest = require('./csi_test.js');

logger.setLevel('debug');

// Function form for terminating assertion properties to make JS linter happy
chai.use(dirtyChai);

describe('moac', function () {
  describe('workq', workqTest);
  describe('grpc client', grpcTest);
  describe('watcher', watcherTest);
  describe('node object', nodeObject);
  describe('pool object', poolObject);
  describe('replica object', replicaObject);
  describe('nats message bus', natsTest);
  describe('nexus object', nexusObject);
  describe('node operator', nodeOperator);
  describe('registry', registryTest);
  describe('event stream', eventStream);
  describe('pool operator', poolOperator);
  describe('volume object', volumeObject);
  describe('volumes', volumesTest);
  describe('volume operator', volumeOperator);
  describe('rest api', restApi);
  describe('csi', csiTest);

  // Start moac without k8s and NATS server just to test basic errors
  it('start moac process', function (done) {
    // Starting moac, which includes loading all NPM modules from disk, takes
    // time when running in docker with FS mounted from non-linux host.
    this.timeout(4000);

    const child = spawn(path.join(__dirname, '..', 'index.js'), [
      '-s',
      // NATS does not run but just to verify that the option works
      '--message-bus=127.0.0.1'
    ]);
    let stderr = '';

    child.stdout.on('data', (data) => {
      if (data.toString().indexOf('🚀') >= 0) {
        child.kill();
      }
    });
    child.stderr.on('data', (data) => {
      stderr += data.toString();
    });
    child.on('close', (code) => {
      if (code === 0) {
        done();
      } else {
        done(new Error(stderr));
      }
    });
  });
});
