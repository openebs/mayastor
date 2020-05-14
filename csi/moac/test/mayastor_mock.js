const _ = require('lodash');
const assert = require('chai').assert;
const path = require('path');
const protoLoader = require('@grpc/proto-loader');
const grpc = require('grpc-uds');
const enums = require('./grpc_enums');

// each stat is incremented by this each time when stat method is called
const STAT_DELTA = 1000;

// The problem is that the grpc server creates the keys from proto file
// even if they don't exist. So we have to test that the key is there
// but also that it has not a default value (empty string, zero, ...).
function assertHasKeys (obj, keys, empty) {
  empty = empty || [];
  for (var key in obj) {
    if (keys.indexOf(key) < 0) {
      assert(
        false,
        'Extra parameter "' + key + '" in object ' + JSON.stringify(obj)
      );
    }
  }
  for (var i = 0; i < keys.length; i++) {
    const key = keys[i];
    const val = obj[key];
    if (
      val == null ||
      // no way to check boolean
      (typeof val === 'string' && val.length === 0 && empty.indexOf(key) < 0) ||
      (typeof val === 'number' && val === 0 && empty.indexOf(key) < 0)
    ) {
      assert(
        false,
        'Missing property ' + key + ' in object ' + JSON.stringify(obj)
      );
    }
  }
}

// Create mayastor mock grpc server with preconfigured storage pool, replica
// and nexus objects. Pools can be added & deleted by means of grpc calls.
// The actual state (i.e. list of pools) can be retrieved by get*() method.
class MayastorServer {
  constructor (endpoint, pools, replicas, nexus) {
    var packageDefinition = protoLoader.loadSync(
      path.join(__dirname, '..', 'proto', 'mayastor_service.proto'),
      {
        keepCase: false,
        longs: Number,
        enums: String,
        defaults: true,
        oneofs: true
      }
    );
    var protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
    var mayastor = protoDescriptor.mayastor_service;
    var srv = new grpc.Server();

    this.pools = _.cloneDeep(pools || []);
    this.replicas = _.cloneDeep(replicas || []);
    this.nexus = _.cloneDeep(nexus || []);
    this.statCounter = 0;

    var self = this;
    srv.addService(mayastor.Mayastor.service, {
      // When a pool is created we implicitly set state to POOL_ONLINE,
      // capacity to 100 and used to 4.
      createPool: (call, cb) => {
        const args = call.request;
        assertHasKeys(
          args,
          ['name', 'disks', 'blockSize', 'ioIf'],
          ['blockSize', 'ioIf']
        );
        if (self.pools.find((p) => p.name === args.name)) {
          const err = new Error('already exists');
          err.code = grpc.status.ALREADY_EXISTS;
          cb(err);
        } else {
          self.pools.push({
            name: args.name,
            disks: args.disks.map((d) => {
              return 'aio://' + d;
            }),
            state: enums.POOL_ONLINE,
            capacity: 100,
            used: 4
          });
          cb(null, {});
        }
      },
      destroyPool: (call, cb) => {
        const args = call.request;
        assertHasKeys(args, ['name']);
        var idx = self.pools.findIndex((p) => p.name === args.name);
        if (idx >= 0) {
          self.pools.splice(idx, 1);
          cb(null, {});
        } else {
          const err = new Error('not found');
          err.code = grpc.status.NOT_FOUND;
          cb(err);
        }
      },
      listPools: (_, cb) => {
        cb(null, { pools: self.pools });
      },
      createReplica: (call, cb) => {
        const args = call.request;
        assertHasKeys(args, ['uuid', 'pool', 'size', 'thin', 'share']);
        if (self.replicas.find((r) => r.uuid === args.uuid)) {
          const err = new Error('already exists');
          err.code = grpc.status.ALREADY_EXISTS;
          return cb(err);
        }
        const pool = self.pools.find((p) => p.name === args.pool);
        if (!pool) {
          const err = new Error('pool not found');
          err.code = grpc.status.NOT_FOUND;
          return cb(err);
        }
        if (!args.thin) {
          pool.used += args.size;
        }
        var uri;
        if (args.share === 'REPLICA_NONE') {
          uri = 'bdev:///' + args.uuid;
        } else if (args.share === 'REPLICA_ISCSI') {
          uri = 'iscsi://192.168.0.1:3800/' + args.uuid;
        } else {
          uri = 'nvmf://192.168.0.1:4020/' + args.uuid;
        }

        self.replicas.push({
          uuid: args.uuid,
          pool: args.pool,
          size: args.size,
          thin: args.thin,
          share: args.share,
          uri
        });
        cb(null, { uri });
      },
      destroyReplica: (call, cb) => {
        var args = call.request;
        assertHasKeys(args, ['uuid']);
        var idx = self.replicas.findIndex((r) => r.uuid === args.uuid);
        if (idx >= 0) {
          const r = self.replicas.splice(idx, 1)[0];
          if (!r.thin) {
            var pool = self.pools.find((p) => p.name === r.pool);
            pool.used -= r.size;
          }
          cb(null, {});
        } else {
          const err = new Error('not found');
          err.code = grpc.status.NOT_FOUND;
          cb(err);
        }
      },
      listReplicas: (_, cb) => {
        cb(null, { replicas: self.replicas });
      },
      statReplicas: (_, cb) => {
        self.statCounter += STAT_DELTA;
        cb(null, {
          replicas: self.replicas.map((r) => {
            return {
              uuid: r.uuid,
              pool: r.pool,
              stats: {
                numReadOps: self.statCounter,
                numWriteOps: self.statCounter,
                bytesRead: self.statCounter,
                bytesWritten: self.statCounter
              }
            };
          })
        });
      },
      shareReplica: (call, cb) => {
        const args = call.request;
        assertHasKeys(args, ['uuid', 'share']);
        const r = self.replicas.find((ent) => ent.uuid === args.uuid);
        if (!r) {
          const err = new Error('not found');
          err.code = grpc.status.NOT_FOUND;
          return cb(err);
        }
        if (args.share === 'REPLICA_NONE') {
          r.uri = 'bdev:///' + r.uuid;
        } else if (args.share === 'REPLICA_ISCSI') {
          r.uri = 'iscsi://192.168.0.1:3800/' + r.uuid;
        } else if (args.share === 'REPLICA_NVMF') {
          r.uri = 'nvmf://192.168.0.1:4020/' + r.uuid;
        } else {
          assert(false, 'Invalid share protocol');
        }
        r.share = args.share;
        cb(null, {
          uri: r.uri
        });
      },
      createNexus: (call, cb) => {
        const args = call.request;
        assertHasKeys(args, ['uuid', 'size', 'children']);
        if (self.nexus.find((r) => r.uuid === args.uuid)) {
          const err = new Error('already exists');
          err.code = grpc.status.ALREADY_EXISTS;
          return cb(err);
        }
        self.nexus.push({
          uuid: args.uuid,
          size: args.size,
          state: enums.NEXUS_ONLINE,
          children: args.children.map((r) => {
            return {
              uri: r,
              state: enums.CHILD_ONLINE
            };
          })
          // device_path omitted
        });
        cb(null, {});
      },
      destroyNexus: (call, cb) => {
        var args = call.request;
        assertHasKeys(args, ['uuid']);
        var idx = self.nexus.findIndex((n) => n.uuid === args.uuid);
        if (idx >= 0) {
          self.nexus.splice(idx, 1);
          cb(null, {});
        } else {
          const err = new Error('not found');
          err.code = grpc.status.NOT_FOUND;
          cb(err);
        }
      },
      listNexus: (_, cb) => {
        cb(null, { nexusList: self.nexus });
      },
      publishNexus: (call, cb) => {
        var args = call.request;
        assertHasKeys(args, ['uuid', 'share', 'key'], ['key']);
        assert.equal(0, args.share); // Must be value of NEXUS_NBD for now
        var idx = self.nexus.findIndex((n) => n.uuid === args.uuid);
        if (idx >= 0) {
          self.nexus[idx].devicePath = '/dev/nbd0';
          cb(null, {
            devicePath: '/dev/nbd0'
          });
        } else {
          const err = new Error('not found');
          err.code = grpc.status.NOT_FOUND;
          cb(err);
        }
      },
      unpublishNexus: (call, cb) => {
        var args = call.request;
        assertHasKeys(args, ['uuid']);
        var idx = self.nexus.findIndex((n) => n.uuid === args.uuid);
        if (idx >= 0) {
          delete self.nexus[idx].devicePath;
          cb(null, {});
        } else {
          const err = new Error('not found');
          err.code = grpc.status.NOT_FOUND;
          cb(err);
        }
      },
      addChildNexus: (call, cb) => {
        var args = call.request;
        assertHasKeys(args, ['uuid', 'uri', 'rebuild']);
        var n = self.nexus.find((n) => n.uuid === args.uuid);
        if (!n) {
          const err = new Error('not found');
          err.code = grpc.status.NOT_FOUND;
          return cb(err);
        }
        if (!n.children.find((ch) => ch.uri === args.uri)) {
          n.children.push({
            uri: args.uri,
            state: enums.CHILD_DEGRADED
          });
        }
        cb();
      },
      removeChildNexus: (call, cb) => {
        var args = call.request;
        assertHasKeys(args, ['uuid', 'uri']);
        var n = self.nexus.find((n) => n.uuid === args.uuid);
        if (!n) {
          const err = new Error('not found');
          err.code = grpc.status.NOT_FOUND;
          return cb(err);
        }
        n.children = n.children.filter((ch) => ch.uri !== args.uri);
        cb();
      },
      // dummy impl to silence the warning about unimplemented method
      childOperation: (_, cb) => {
        cb();
      }
    });
    srv.bind(endpoint, grpc.ServerCredentials.createInsecure());
    this.srv = srv;
  }

  getPools () {
    return this.pools;
  }

  getReplicas () {
    return this.replicas;
  }

  getNexus () {
    return this.nexus;
  }

  start () {
    this.srv.start();
    return this;
  }

  stop () {
    this.srv.forceShutdown();
  }
}

module.exports = {
  MayastorServer,
  STAT_DELTA
};
