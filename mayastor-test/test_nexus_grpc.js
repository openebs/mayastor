'use strict';

const assert = require('chai').assert;
const async = require('async');
const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');
const { createClient } = require('grpc-kit');
const grpc = require('grpc');
const common = require('./test_common');
const sudo = require('./sudo');

// tunables of the test suite
var endpoint = process.env.MAYASTOR_ENDPOINT;

let config = `
[ISCSI]
  NodeBase "iqn.2016-06.io.openebs"
  # Socket I/O timeout sec. (0 is infinite)
  Timeout 30
  DiscoveryAuthMethod None
  DefaultTime2Wait 2
  DefaultTime2Retain 60
  ImmediateData Yes
  ErrorRecoveryLevel 0

[Malloc]
  NumberOfLuns 2
  LunSizeInMB  64

[PortalGroup1]
  Portal GR1 0.0.0.0:3261

[InitiatorGroup1]
  InitiatorName Any
  Netmask 127.0.0.1/24

[TargetNode0]
  TargetName "iqn.2016-06.io.openebs:disk0"
  TargetAlias "Data Disk0"
  Mapping PortalGroup1 InitiatorGroup1
  AuthMethod None
  UseDigest Auto
  LUN0 Malloc0
  QueueDepth 128


[TargetNode1]
  TargetName "iqn.2016-06.io.openebs:disk1"
  TargetAlias "Data Disk1"
  Mapping PortalGroup1 InitiatorGroup1
  AuthMethod None
  LUN0 Malloc1
  QueueDepth 128
`;

let nbd_device;

after(common.stopMayastor);

describe('nexus_grpc', function() {
  this.timeout(200000); // for network e2e tests we need long timeouts

  // start mayastor if needed
  before(() => {
    // if no explicit gRPC endpoint given then create one by starting
    // mayastor and grpc server
    if (!endpoint) {
      endpoint = common.endpoint;
      common.startMayastor(config);
      common.startMayastorGrpc();
    }
  });

  // stop mayastor server if it was started by us
  after(common.stopMayastor);

  describe('nexus', function() {
    var client;

    before(done => {
      client = createClient(
        {
          protoPath: path.join(
            __dirname,
            '..',
            'rpc',
            'proto',
            'mayastor_service.proto'
          ),
          packageName: 'mayastor_service',
          serviceName: 'Mayastor',
          options: {
            keepCase: true,
            longs: String,
            enums: String,
            defaults: true,
            oneofs: true,
          },
        },
        endpoint
      );

      if (!client) {
        return done(new Error('Failed to initialize grpc client'));
      }

      async.series(
        [
          next => {
            common.waitForMayastor(pingDone => {
              // use harmless method to test if the mayastor is up and running
              client.listPools({}, pingDone);
            }, next);
          },
          next => {
            // We need to read/write the raw device from test suite
            let child = sudo(['sh', '-c', 'chmod o+rw /dev/nbd*']);
            child.stderr.on('data', data => {
              console.log(data.toString());
            });
            child.on('close', (code, signal) => {
              if (code != 0) {
                next(new Error('Failed to chmod nbd devs'));
              } else {
                next();
              }
            });
          },
        ],
        done
      );
    });

    after(done => {
      async.series(
        [
          next => {
            // Undo change of permissions on /dev/nbd*
            let child = sudo(['sh', '-c', 'chmod o-rw /dev/nbd*']);
            child.on('close', (code, signal) => {
              if (code != 0) {
                next(new Error('Failed to chmod nbd devs'));
              } else {
                next();
              }
            });
          },
        ],
        err => {
          if (client != null) {
            client.close();
          }
          done(err);
        }
      );
    });

    it('Should not list any nexus devices', done => {
      client.ListNexus({}, (err, res) => {
        assert(res.nexus_list.length === 0);
        done();
      });
    });

    it('it should be able to create a Nexus using two iSCSI URI', done => {
      let args = {
        name: 'nexus0',
        size: 131072,
        block_len: 512,
        replicas: [
          'iscsi://127.0.0.1:3261/iqn.2016-06.io.openebs:disk0',
          'iscsi://127.0.0.1:3261/iqn.2016-06.io.openebs:disk1',
        ],
      };

      client.CreateNexus(args, (err, res) => {
        assert(res.name === 'nexus0');
        done();
      });
    });

    it('should be able to list the created nexus nexus0', done => {
      client.ListNexus({}, (err, res) => {
        assert(res.nexus_list.length !== 0);

        let nexus = res.nexus_list[0];

        assert(nexus.name === 'nexus0');
        assert(nexus.state === 'online');
        assert(nexus.children.length === 2);
        assert(nexus.children[0].state === nexus.children[1].state);
        done();
      });
    });

    it('should succeed creating the same nexus0 again', done => {
      let args = {
        name: 'nexus0',
        size: 131072,
        block_len: 512,
        replicas: [
          'iscsi://127.0.0.1:3261/iqn.2016-06.io.openebs:disk0',
          'iscsi://127.0.0.1:3261/iqn.2016-06.io.openebs:disk1',
        ],
      };

      client.CreateNexus(args, (err, res) => {
        assert(res.name === 'nexus0');
        done();
      });
    });

    it('should succeed creating the same nexus nexus0 again but with different URIs', done => {
      let args = {
        name: 'nexus0',
        size: 131072,
        block_len: 512,
        replicas: [
          'iscsi://127.0.0.1:3261/iqn.2016-06.io.openebs:disk2',
          'iscsi://127.0.0.1:3261/iqn.2016-06.io.openebs:disk3',
        ],
      };

      client.CreateNexus(args, (err, res) => {
        assert(res.name === 'nexus0');
        done();
      });
    });

    it('should fail to create a nexus: nexus1  with in use URIs', done => {
      let args = {
        name: 'nexus1',
        size: 131072,
        block_len: 512,
        replicas: [
          'iscsi://127.0.0.1:3261/iqn.2016-06.io.openebs:disk0',
          'iscsi://127.0.0.1:3261/iqn.2016-06.io.openebs:disk1',
        ],
      };

      client.CreateNexus(args, (err, res) => {
        assert(err.code === 13);
        done();
      });
    });

    it('should fail creating a nexus with non existing URIs', done => {
      let args = {
        name: 'nexus1',
        size: 131072,
        block_len: 512,
        replicas: [
          'iscsi://127.0.0.1:3261/iqn.2016-06.io.spdk:disk2',
          'iscsi://127.0.0.1:3261/iqn.2016-06.io.spdk:disk3',
        ],
      };

      client.CreateNexus(args, (err, res) => {
        assert(err.code === 13);
        done();
      });
    });

    it('should be able to publish a nexus device using nbd', done => {
      client.PublishNexus({ bdev_name: 'nexus0' }, (err, res) => {
        assert(res.device_path);
        nbd_device = res.device_path;
        done();
      });
    });

    it('should be able to write to the nbd device', done => {
      fs.open(nbd_device, 'w', 666, (err, fd) => {
        if (err) return done(err);
        let buffer = Buffer.alloc(512, 'z', 'utf8');
        fs.write(fd, buffer, 0, 512, (err, nr, buffer) => {
          if (err) return done(err);
          assert(nr === 512);
          assert(buffer[0] === 122);
          assert(buffer[511] === 122);
          fs.fsync(fd, err => {
            if (err) done(err);
            fs.close(fd, () => {
              done();
            });
          });
        });
      });
    });

    it('should be able to read the written data back', done => {
      fs.open(nbd_device, 'r', 666, (err, fd) => {
        if (err) done(err);
        let buffer = Buffer.alloc(512, 'a', 'utf8');

        fs.read(fd, buffer, 0, 512, 0, (err, nr, buffer) => {
          if (err) done(err);

          /*
           * just check the last two bytes of the buffer as its not a data integrity test
           */

          assert(buffer[0] == 122);
          assert(buffer[511] == 122);
          fs.close(fd, () => {
            done();
          });
        });
      });
    });

    it('should be able to destroy the nexus', done => {
      client.DestroyNexus({ name: 'nexus0' }, (err, res) => {
        if (err) done(err);
        done();
      });
    });
  });
});
