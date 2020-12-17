'use strict';

// Test NATS message bus implementation in mayastor.

const assert = require('chai').assert;
const { spawn } = require('child_process');
const common = require('./test_common');
const nats = require('nats');

const HB_INTERVAL = 1;
const NATS_PORT = 14222;
const NATS_ENDPOINT = common.getMyIp() + ':' + NATS_PORT;
const NODE_NAME = 'weird-node-name';

let natsProc;

// start nats server
function startNats (done) {
  natsProc = spawn('nats-server', ['-a', common.getMyIp(), '-p', NATS_PORT]);
  let doneCalled = false;
  let stderr = '';

  natsProc.stderr.on('data', (data) => {
    stderr += data.toString();
    if (data.toString().match(/Server is ready/)) {
      doneCalled = true;
      done();
    }
  });

  natsProc.once('close', (code) => {
    natsProc = null;
    if (!doneCalled) {
      if (code) {
        done(new Error(`nats server exited with code ${code}: ${stderr}`));
      } else {
        done(new Error('nats server exited prematurely'));
      }
      return;
    }
    if (code) {
      console.log(`nats server exited with code ${code}: ${stderr}`);
    }
  });
}

// stop nats server
function stopNats (done) {
  if (!natsProc) return done();
  natsProc.once('close', () => done());
  natsProc.kill();
}

function assertRegisterMessage (msg) {
  assert.strictEqual(JSON.parse(msg).id, 'v0/register');
  const args = JSON.parse(msg).data;
  assert.hasAllKeys(args, ['id', 'grpcEndpoint']);
  assert.strictEqual(args.id, NODE_NAME);
  assert.strictEqual(args.grpcEndpoint, common.grpcEndpoint);
}

// The tests must be run in sequence. We start/stop mayastor and NATS as part
// of the tests and setting the right environment for each test would be
// tedious.
describe('nats', function () {
  let client;

  // longer timeout - the tests wait for register messages
  this.timeout(5000);

  before(startNats);

  after((done) => {
    if (client != null) {
      client.close();
      client = null;
    }
    stopNats(() => common.stopAll(done));
  });

  it('should send a registration message when mayastor starts', (done) => {
    client = nats.connect(`nats://${NATS_ENDPOINT}`);
    client.on('connect', () => {
      // start mayastor
      common.startMayastor(null, [
        '-g', common.grpcEndpoint,
        '-n', NATS_ENDPOINT,
        '-N', NODE_NAME
      ], {
        MAYASTOR_HB_INTERVAL: HB_INTERVAL
      });
      // wait for the register message
      const sid = client.subscribe('v0/registry', (msg) => {
        client.unsubscribe(sid);
        assertRegisterMessage(msg);
        done();
      });
    });
  });

  it('should keep sending registration messages', (done) => {
    const sid = client.subscribe('v0/registry', (msg) => {
      client.unsubscribe(sid);
      assertRegisterMessage(msg);
      done();
    });
  });

  it('should send a registration message after NATS becomes available again', (done) => {
    // simulate outage of NATS server for a duration of two heartbeats
    stopNats(() => {
      setTimeout(() => {
        const sid = client.subscribe('v0/registry', (msg) => {
          client.unsubscribe(sid);
          assertRegisterMessage(msg);
          done();
        });
        startNats((err) => {
          if (err) done(err);
        });
      }, 2 * HB_INTERVAL);
    });
  });

  it('should send a deregistration message when mayastor is shut down', (done) => {
    const sid = client.subscribe('v0/registry', (msg) => {
      client.unsubscribe(sid);
      assert.strictEqual(JSON.parse(msg).id, 'v0/deregister');
      const args = JSON.parse(msg).data;
      assert.hasAllKeys(args, ['id']);
      assert.strictEqual(args.id, NODE_NAME);
      done();
    });
    common.stopAll((err) => {
      if (err) done(err);
    });
  });
});
