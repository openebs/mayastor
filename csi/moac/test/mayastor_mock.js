const _ = require('lodash');
const assert = require('chai').assert;
const path = require('path');
const protoLoader = require('@grpc/proto-loader');
const grpc = require('@grpc/grpc-js');
const enums = require('./grpc_enums');
const parse = require('url-parse');

// each stat is incremented by this each time when stat method is called
const STAT_DELTA = 1000;

// The problem is that the grpc server creates the keys from proto file
// even if they don't exist. So we have to test that the key is there
// but also that it has not a default value (empty string, zero, ...).
function assertHasKeys (obj, keys, empty) {
  empty = empty || [];
  for (const key in obj) {
    if (keys.indexOf(key) < 0) {
      assert(
        false,
        'Extra parameter "' + key + '" in object ' + JSON.stringify(obj)
      );
    }
  }
  for (let i = 0; i < keys.length; i++) {
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
  constructor (endpoint, pools, replicas, nexus, replyDelay) {
    const packageDefinition = protoLoader.loadSync(
      path.join(__dirname, '..', 'proto', 'mayastor.proto'),
      {
        keepCase: false,
        longs: Number,
        enums: String,
        defaults: true,
        oneofs: true
      }
    );
    const mayastor = grpc.loadPackageDefinition(packageDefinition).mayastor;
    const srv = new grpc.Server();

    this.endpoint = endpoint;
    this.pools = _.cloneDeep(pools || []);
    this.replicas = _.cloneDeep(replicas || []);
    this.nexus = _.cloneDeep(nexus || []);
    this.statCounter = 0;
    const randomUuidQp = () => {
      return '?uuid=' + _.random(0, Number.MAX_SAFE_INTEGER);
    };
    const uuidQp = (uuid) => {
      return '?uuid=' + uuid;
    };
    if (replyDelay == null) {
      replyDelay = 0;
    }

    const self = this;
    srv.addService(mayastor.Mayastor.service, {
      // When a pool is created we implicitly set state to POOL_ONLINE,
      // capacity to 100 and used to 4.
      createPool: (call, cb) => {
        const args = call.request;
        assertHasKeys(
          args,
          ['name', 'disks'],
          []
        );
        let pool = self.pools.find((p) => p.name === args.name);
        if (!pool) {
          pool = {
            name: args.name,
            disks: args.disks.map((d) => `aio://${d}`),
            state: enums.POOL_ONLINE,
            capacity: 100,
            used: 4
          };
          self.pools.push(pool);
        }
        setTimeout(() => cb(null, pool), replyDelay);
      },
      destroyPool: (call, cb) => {
        const args = call.request;
        assertHasKeys(args, ['name']);
        const idx = self.pools.findIndex((p) => p.name === args.name);
        if (idx >= 0) {
          self.pools.splice(idx, 1);
        }
        setTimeout(() => cb(null, {}), replyDelay);
      },
      listPools: (_unused, cb) => {
        setTimeout(() => cb(null, { pools: self.pools }), replyDelay);
      },
      createReplica: (call, cb) => {
        const args = call.request;
        assertHasKeys(args, ['uuid', 'pool', 'size', 'thin', 'share']);
        let r = self.replicas.find((r) => r.uuid === args.uuid);
        if (r) {
          return setTimeout(() => cb(null, r), replyDelay);
        }
        const pool = self.pools.find((p) => p.name === args.pool);
        if (!pool) {
          const err = new Error('pool not found');
          err.code = grpc.status.NOT_FOUND;
          return setTimeout(() => cb(err), replyDelay);
        }
        if (!args.thin) {
          pool.used += args.size;
        }
        let uri;
        if (args.share === 'REPLICA_NONE') {
          uri = 'bdev:///' + args.uuid + randomUuidQp();
        } else if (args.share === 'REPLICA_ISCSI') {
          uri = 'iscsi://192.168.0.1:3800/' + args.uuid + randomUuidQp();
        } else {
          uri = 'nvmf://192.168.0.1:4020/' + args.uuid + randomUuidQp();
        }

        r = {
          uuid: args.uuid,
          pool: args.pool,
          size: args.size,
          thin: args.thin,
          share: args.share,
          uri
        };
        self.replicas.push(r);
        setTimeout(() => cb(null, r), replyDelay);
      },
      destroyReplica: (call, cb) => {
        const args = call.request;
        assertHasKeys(args, ['uuid']);
        const idx = self.replicas.findIndex((r) => r.uuid === args.uuid);
        if (idx >= 0) {
          const r = self.replicas.splice(idx, 1)[0];
          if (!r.thin) {
            const pool = self.pools.find((p) => p.name === r.pool);
            pool.used -= r.size;
          }
        }
        setTimeout(() => cb(null, {}), replyDelay);
      },
      listReplicas: (_unused, cb) => {
        setTimeout(() => cb(null, { replicas: self.replicas }), replyDelay);
      },
      statReplicas: (_unused, cb) => {
        self.statCounter += STAT_DELTA;
        setTimeout(() => cb(null, {
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
        }), replyDelay);
      },
      shareReplica: (call, cb) => {
        const args = call.request;
        assertHasKeys(args, ['uuid', 'share']);
        const r = self.replicas.find((ent) => ent.uuid === args.uuid);
        if (!r) {
          const err = new Error('not found');
          err.code = grpc.status.NOT_FOUND;
          return setTimeout(() => cb(err), replyDelay);
        }
        assertHasKeys(r, ['uri']);
        const realUuid = parse(r.uri, true).query.uuid;
        if (args.share === 'REPLICA_NONE') {
          r.uri = 'bdev:///' + uuidQp(realUuid);
        } else if (args.share === 'REPLICA_ISCSI') {
          r.uri = 'iscsi://192.168.0.1:3800/' + r.uuid + uuidQp(realUuid);
        } else if (args.share === 'REPLICA_NVMF') {
          r.uri = 'nvmf://192.168.0.1:4020/' + r.uuid + uuidQp(realUuid);
        } else {
          assert(false, 'Invalid share protocol');
        }
        r.share = args.share;
        setTimeout(() => cb(null, { uri: r.uri }), replyDelay);
      },
      createNexus: (call, cb) => {
        const args = call.request;
        assertHasKeys(args, ['uuid', 'size', 'children']);
        let nexus = self.nexus.find((r) => r.uuid === args.uuid);
        if (!nexus) {
          nexus = {
            uuid: args.uuid,
            size: args.size,
            state: enums.NEXUS_ONLINE,
            children: args.children.map((r) => {
              return {
                uri: r,
                state: enums.CHILD_ONLINE,
                rebuildProgress: 0
              };
            })
            // device_path omitted
          };
          self.nexus.push(nexus);
        }
        setTimeout(() => cb(null, nexus), replyDelay);
      },
      destroyNexus: (call, cb) => {
        const args = call.request;
        assertHasKeys(args, ['uuid']);
        const idx = self.nexus.findIndex((n) => n.uuid === args.uuid);
        if (idx >= 0) {
          self.nexus.splice(idx, 1);
        }
        setTimeout(() => cb(null, {}), replyDelay);
      },
      listNexus: (_unused, cb) => {
        setTimeout(() => cb(null, { nexusList: self.nexus }), replyDelay);
      },
      publishNexus: (call, cb) => {
        const args = call.request;
        assertHasKeys(args, ['uuid', 'share', 'key'], ['key']);
        assert.equal(1, args.share); // Must be value of NEXUS_NVMF for now
        const idx = self.nexus.findIndex((n) => n.uuid === args.uuid);
        if (idx >= 0) {
          self.nexus[idx].deviceUri = 'nvmf://host/nqn';
          setTimeout(() => cb(null, {
            deviceUri: 'nvmf://host/nqn'
          }), replyDelay);
        } else {
          const err = new Error('not found');
          err.code = grpc.status.NOT_FOUND;
          setTimeout(() => cb(err), replyDelay);
        }
      },
      unpublishNexus: (call, cb) => {
        const args = call.request;
        assertHasKeys(args, ['uuid']);
        const idx = self.nexus.findIndex((n) => n.uuid === args.uuid);
        if (idx >= 0) {
          delete self.nexus[idx].deviceUri;
          setTimeout(() => cb(null, {}), replyDelay);
        } else {
          const err = new Error('not found');
          err.code = grpc.status.NOT_FOUND;
          setTimeout(() => cb(err), replyDelay);
        }
      },
      addChildNexus: (call, cb) => {
        const args = call.request;
        assertHasKeys(args, ['uuid', 'uri', 'norebuild']);
        const n = self.nexus.find((n) => n.uuid === args.uuid);
        if (!n) {
          const err = new Error('not found');
          err.code = grpc.status.NOT_FOUND;
          return setTimeout(() => cb(err), replyDelay);
        }
        if (!n.children.find((ch) => ch.uri === args.uri)) {
          n.children.push({
            uri: args.uri,
            state: enums.CHILD_DEGRADED
          });
        }
        setTimeout(() => cb(null, {
          uri: args.uri,
          state: enums.CHILD_DEGRADED,
          rebuildProgress: 0
        }), replyDelay);
      },
      removeChildNexus: (call, cb) => {
        const args = call.request;
        assertHasKeys(args, ['uuid', 'uri']);
        const n = self.nexus.find((n) => n.uuid === args.uuid);
        if (!n) {
          const err = new Error('not found');
          err.code = grpc.status.NOT_FOUND;
          return setTimeout(() => cb(err), replyDelay);
        }
        n.children = n.children.filter((ch) => ch.uri !== args.uri);
        setTimeout(cb, replyDelay);
      },
      // dummy impl to silence the warning about unimplemented method
      childOperation: (_unused, cb) => {
        setTimeout(cb, replyDelay);
      }
    });
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

  start (done) {
    this.srv.bindAsync(
      this.endpoint,
      grpc.ServerCredentials.createInsecure(),
      (err) => {
        if (err) return done(err);
        this.srv.start();
        done();
      });
  }

  stop () {
    this.srv.forceShutdown();
  }
}

module.exports = {
  MayastorServer,
  STAT_DELTA
};
