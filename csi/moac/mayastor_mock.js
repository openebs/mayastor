const assert = require('chai').assert;
const path = require('path');
const protoLoader = require('@grpc/proto-loader');
const grpc = require('grpc');

// each stat is incremented by this each time when stat method is called
const STAT_DELTA = 1000;

// Create mayastor mock grpc server with preconfigured storage pools and nexus
// objects. Pools can be added & deleted by means of grpc calls. The actual
// state (list of pools) can be retrieved by get() method.
class MayastorServer {
  constructor(endpoint, pools, replicas, nexus) {
    var packageDefinition = protoLoader.loadSync(
      path.join(__dirname, '../', '../rpc', 'proto', 'mayastor_service.proto'),
      {
        keepCase: false,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
      }
    );
    var protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
    var mayastor = protoDescriptor.mayastor_service;
    var srv = new grpc.Server();

    this.pools = pools || [];
    this.replicas = replicas || [];
    this.nexus = nexus || [];
    this.statCounter = 0;

    var self = this;
    srv.addService(mayastor.Mayastor.service, {
      // When a pool is created we implicitly set state to ONLINE,
      // capacity to 100 and used to 4.
      createPool: (call, cb) => {
        let args = call.request;
        assert.hasAllKeys(args, ['name', 'disks', 'blockSize']);
        if (self.pools.find(p => p.name == args.name)) {
          let err = new Error('already exists');
          err.code = grpc.status.ALREADY_EXISTS;
          cb(err);
        } else {
          self.pools.push({
            name: args.name,
            disks: args.disks,
            state: 0,
            capacity: 100,
            used: 4,
          });
          cb(null, {});
        }
      },
      destroyPool: (call, cb) => {
        let args = call.request;
        assert.hasAllKeys(args, ['name']);
        var idx = self.pools.findIndex(p => p.name == args.name);
        if (idx >= 0) {
          self.pools.splice(idx, 1);
          cb(null, {});
        } else {
          let err = new Error('not found');
          err.code = grpc.status.NOT_FOUND;
          cb(err);
        }
      },
      listPools: (_, cb) => {
        cb(null, { pools: self.pools });
      },
      createReplica: (call, cb) => {
        let args = call.request;
        assert.hasAllKeys(args, ['uuid', 'pool', 'size', 'thin', 'share']);
        if (self.replicas.find(r => r.uuid == args.uuid)) {
          let err = new Error('already exists');
          err.code = grpc.status.ALREADY_EXISTS;
          return cb(err);
        }
        let pool = self.pools.find(p => p.name == args.pool);
        if (!pool) {
          let err = new Error('pool not found');
          err.code = grpc.status.NOT_FOUND;
          return cb(err);
        }
        if (!args.thin) {
          pool.used += args.size;
        }

        self.replicas.push({
          uuid: args.uuid,
          pool: args.pool,
          size: args.size,
          thin: args.thin,
          share: args.share,
        });
        cb(null, {});
      },
      destroyReplica: (call, cb) => {
        var args = call.request;
        assert.hasAllKeys(args, ['uuid']);
        var idx = self.replicas.findIndex(r => r.uuid == args.uuid);
        if (idx >= 0) {
          let r = self.replicas.splice(idx, 1)[0];
          if (!r.thin) {
            var pool = self.pools.find(p => p.name == r.pool);
            pool.used -= r.size;
          }
          cb(null, {});
        } else {
          let err = new Error('not found');
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
          replicas: self.replicas.map(r => {
            return {
              uuid: r.uuid,
              pool: r.pool,
              stats: {
                numReadOps: self.statCounter,
                numWriteOps: self.statCounter,
                bytesRead: self.statCounter,
                bytesWritten: self.statCounter,
              },
            };
          }),
        });
      },
      createNexus: (call, cb) => {
        let args = call.request;
        assert.hasAllKeys(args, ['uuid', 'size', 'children']);
        if (self.nexus.find(r => r.uuid == args.uuid)) {
          let err = new Error('already exists');
          err.code = grpc.status.ALREADY_EXISTS;
          return cb(err);
        }
        self.nexus.push({
          uuid: args.uuid,
          size: args.size,
          state: 'online',
          children: args.children.map(r => {
            return {
              uri: r,
              state: 'online',
            };
          }),
          // device_path omitted
        });
        cb(null, {});
      },
      destroyNexus: (call, cb) => {
        var args = call.request;
        assert.hasAllKeys(args, ['uuid']);
        var idx = self.nexus.findIndex(n => n.uuid == args.uuid);
        if (idx >= 0) {
          let n = self.nexus.splice(idx, 1)[0];
          cb(null, {});
        } else {
          let err = new Error('not found');
          err.code = grpc.status.NOT_FOUND;
          cb(err);
        }
      },
      listNexus: (_, cb) => {
        cb(null, { nexusList: self.nexus });
      },
      publishNexus: (call, cb) => {
        var args = call.request;
        assert.hasAllKeys(args, ['uuid']);
        var idx = self.nexus.findIndex(n => n.uuid == args.uuid);
        if (idx >= 0) {
          self.nexus[idx].devicePath = '/dev/nbd0';
          cb(null, {
            devicePath: '/dev/nbd0',
          });
        } else {
          let err = new Error('not found');
          err.code = grpc.status.NOT_FOUND;
          cb(err);
        }
      },
      unpublishNexus: (call, cb) => {
        var args = call.request;
        assert.hasAllKeys(args, ['uuid']);
        var idx = self.nexus.findIndex(n => n.uuid == args.uuid);
        if (idx >= 0) {
          delete self.nexus[idx].devicePath;
          cb(null, {});
        } else {
          let err = new Error('not found');
          err.code = grpc.status.NOT_FOUND;
          cb(err);
        }
      },
      // dummy impl to silence the warning about unimplemented method
      childOperation: (_, cb) => {
        cb();
      },
    });
    srv.bind(endpoint, grpc.ServerCredentials.createInsecure());
    this.srv = srv;
  }

  getPools() {
    return this.pools;
  }

  getReplicas() {
    return this.replicas;
  }

  getNexus() {
    return this.nexus;
  }

  start() {
    this.srv.start();
    return this;
  }

  stop() {
    this.srv.forceShutdown();
  }
}

module.exports = {
  MayastorServer,
  STAT_DELTA,
};
