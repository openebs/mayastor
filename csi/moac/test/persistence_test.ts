// Unit tests for the persistent store and its etcd client

import { Etcd3, IOptions, isRecoverableError } from 'etcd3';
import { defaults } from 'lodash';
import { Done } from 'mocha';
import { spawn, ChildProcessWithoutNullStreams } from 'child_process';
import { expect } from 'chai';
import { persistence } from '../proto/generated/persistence'
import { rmdirSync } from 'fs';
import { Replica } from '../replica';
const util = require('util');
import { Policy, ConsecutiveBreaker } from 'cockatiel';
import * as sinon from 'ts-sinon';
import { PersistentStore } from '../persistent_store';

const ETCD_STORE = "/tmp/moac-etcd-test";
const ETCD_PORT = '2379';
const ETCD_HOST = '127.0.0.1';
const ETCD_EP = `${ETCD_HOST}:${ETCD_PORT}`;

let etcdProc: ChildProcessWithoutNullStreams | null;
// Starts etcd server and calls the callback when the server is up and ready.
function startEtcd (done: Done) {
  if (etcdProc != null) {
    done();
    return;
  }
  rmdirSync(ETCD_STORE, { recursive: true });
  etcdProc = spawn('etcd', ['--data-dir', ETCD_STORE]);
  let doneCalled = false;
  let stderr = '';

  etcdProc?.stderr.on('data', (data: any) => {
    stderr += data.toString();
    if (data.toString().match(/ready to serve client requests/)) {
      doneCalled = true;
      done();
    }
  });

  etcdProc?.once('close', (code: any) => {
    etcdProc = null;
    if (!doneCalled) {
      if (code) {
        done(new Error(`etcd server exited with code ${code}: ${stderr}`));
      } else {
        done(new Error('etcd server exited prematurely'));
      }
      return;
    }
    if (code) {
      console.log(`etcd server exited with code ${code}: ${stderr}`);
    }
  });
}

// Kill etcd server. Though it does not wait for it to exit!
function stopEtcd () {
  etcdProc?.kill();
  rmdirSync(ETCD_STORE, { recursive: true });
}

module.exports = function () {
  // adds all possible combinations of child state and reason to the NexusInfo
  function add_children_combinations(nexusInfo: persistence.NexusInfo) {
    for (const state in Object.values(persistence.ChildState)) {
      for (const reason in Object.values(persistence.Reason)) {
        var child = new persistence.ChildInfo({
          state: Object.values(persistence.ChildState)[state] as persistence.ChildState,
          reason: Object.values(persistence.Reason)[reason] as persistence.Reason,
          uuid: nexusInfo.children.length.toString(),
        });
        nexusInfo.children.push(child);
      }
    }
  }

  // returns a NexusInfo with the given clean_shutdown flag and twice all possible children combinations
  function get_nexus_info(clean_shutdown: boolean): persistence.NexusInfo {
    let nexusInfo = new persistence.NexusInfo({
      clean_shutdown: clean_shutdown,
      children: []
    });

    // add 2 of each
    add_children_combinations(nexusInfo);
    add_children_combinations(nexusInfo);

    return nexusInfo;
  }

  describe('with real etcd server', () => {
    let client = new Etcd3(getOptions());

    function getOptions(): IOptions {
      return {
        hosts: ETCD_EP,
        faultHandling: {
           host: () =>
             // make sure the circuit breaker does not kick in right away for most tests
             Policy.handleWhen(isRecoverableError).circuitBreaker(1_000, new ConsecutiveBreaker(10)),
          global: Policy.handleWhen(isRecoverableError).retry().attempts(3),
        },
        ...defaults
      };
    }

    beforeEach((done) => {
      startEtcd(async (err: any) => {
        if (err) return done(err);
        // clear up etcd
        await client.delete().all();
        done();
      });
    });

    after(() => {
      stopEtcd();
    });

    it('should read NexusInfo from the persistent store', async () => {
      let uuid = "1";

      let nexus_not_there = await client.get(uuid);
      expect(nexus_not_there).to.be.null;

      // now put it there
      // todo: use number format for the enums
      await client.put(uuid).value(JSON.stringify(get_nexus_info(true)));
      // and read it back
      let nexus = await client.get(uuid).json() as persistence.NexusInfo;

      expect(nexus).not.to.be.null;
      // inner values should match up
      expect(nexus.children.values).equals(get_nexus_info(true).children.values);
    });

    it('should throw if etcd is not reachable', async () => {
      const persistent_store = new PersistentStore([], 1000, () => client);

      stopEtcd();
      let has_thrown = false;
      try {
        await persistent_store.filter_replicas("1", []);
      } catch (error: any) {
        has_thrown = true;
      }
      expect(has_thrown).to.be.true;

      // start etcd again
      await new Promise((resolve: (res: void) => void) => {
        startEtcd(() => {
          resolve();
        });
      });

      has_thrown = false;
      try {
        await persistent_store.filter_replicas("1", []);
      } catch (error: any) {
        console.log(`Caught unexpected exception, error: ${error}`);
        has_thrown = true;
      }
      expect(has_thrown).to.be.false;
    });

    it('should delete NexusInfo from the persistent store', async () => {
      let uuid = "1";

      let nexus_not_there = await client.get(uuid);
      expect(nexus_not_there).to.be.null;

      // now put it there
      await client.put(uuid).value(JSON.stringify(get_nexus_info(true)));
      // and read it back
      let nexus = await client.get(uuid).json() as persistence.NexusInfo;
      expect(nexus).not.to.be.null;

      const persistent_store = new PersistentStore([], 1000, () => client);
      await persistent_store.destroy_nexus(uuid);

      nexus_not_there = await client.get(uuid);
      expect(nexus_not_there).to.be.null;
    });
  });

  describe('with mock etcd client', () => {
    const client = new Etcd3();
    const persistent_store = new PersistentStore([], 1000, () => client);

    it('should mock the persistent store', async () => {
      // hint: remove any cast to figure out which calls exec
      const mock = client.mock({ exec: sinon.default.stub() as any});
      mock.exec.callsFake((_serviceName:any, method:string, payload:any) => {
        if (method === 'range' && payload.key == 'foo')  {
          return {
            kvs: [{ value: 'bar' }]
          };
        } else
          return {
            kvs: [{ value: 'bar_not_foo' }]
          };
      });
      let output = await client.get('foo');
      expect(output).to.equal('bar');
      output = await client.get('foos');
      expect(output).to.equal('bar_not_foo');
      client.unmock();
    });

    it('should throw if the persistent store has invalid data', async () => {
      const mock = client.mock({ exec: sinon.default.stub() as any});
      let replicas = [new Replica({ uri: 'bdev:///1?uuid=1' }), new Replica({ uri: 'bdev:///1?uuid=2' })];

      // not a valid json
      mock.exec.resolves({ kvs: [{ value: 'not json' }] });
      let has_thrown = false;
      try {
        await persistent_store.filter_replicas("1", replicas);
      } catch (error: any) {
        has_thrown = true;
      }
      expect(has_thrown).to.be.true;

      // valid json but not in the right format
      mock.exec.resolves({ kvs: [{ value: '{ "clean_shutdowns": true, "children": [] }' }] });
      has_thrown = false;
      try {
        await persistent_store.filter_replicas("1", replicas);
      } catch (error: any) {
        has_thrown = true;
      }
      expect(has_thrown).to.be.true;
      mock.exec.resolves({ kvs: [{ value: '{ "clean_shutdown": true, "childrens": [] }' }] });
      has_thrown = false;
      try {
        await persistent_store.filter_replicas("1", replicas);
      } catch (error: any) {
        has_thrown = true;
      }
      expect(has_thrown).to.be.true;

      // valid json and in the right format, so we should not throw now
      mock.exec.resolves({ kvs: [{ value: '{ "clean_shutdown": true, "children": [] }' }] });
      await persistent_store.filter_replicas("1", replicas);
    });

    it('should not filter out replicas on the first nexus creation', async () => {
      const mock = client.mock({ exec: sinon.default.stub() as any});
      mock.exec.resolves({ kvs: [] });
      let replicas = [new Replica({ uri: 'bdev:///1?uuid=1' }), new Replica({ uri: 'bdev:///1?uuid=2' })];
      let replicas_filtered = await persistent_store.filter_replicas("1", replicas);
      expect(replicas_filtered).equals(replicas);
    });

    it('should return a single healthy child on an unclean shutdown of the nexus', async () => {
      const mock = client.mock({ exec: sinon.default.stub() as any});
      let replicas = [new Replica({ uri: 'bdev:///1?uuid=1' }), new Replica({ uri: 'bdev:///1?uuid=2' })];

      // no children at all in the nexus, which is strange, but nonetheless, means we cannot create the nexus
      mock.exec.resolves({ kvs: [{ value: '{ "clean_shutdown": false, "children": [] }' }] });
      let replicas_filtered = await persistent_store.filter_replicas("1", replicas);
      expect(replicas_filtered.length).equals(0);

      let nexus = get_nexus_info(false);
      mock.exec.resolves({ kvs: [{ value: JSON.stringify(nexus) }] });
      let open_children = nexus.children.filter((c) => {
        return c.state === persistence.ChildState.Open;
      });

      replicas = open_children.map((c) => {
        return new Replica({ uri: `bdev:///1?uuid=${c.uuid}` });
      });
      expect(replicas.length).greaterThan(1);

      replicas_filtered = await persistent_store.filter_replicas("1", replicas);
      expect(replicas_filtered.length).equals(1);
      let child = open_children.find((c) => replicas_filtered[0].realUuid === c.uuid);
      expect(child).not.to.be.undefined;
      expect(child?.state).equals(persistence.ChildState.Open);
    });

    it('should return only healthy children on a clean shutdown of the nexus', async () => {
      const mock = client.mock({ exec: sinon.default.stub() as any});

      let nexus = get_nexus_info(true);
      mock.exec.resolves({ kvs: [{ value: JSON.stringify(nexus) }] });
      let open_children = nexus.children.filter((c) => {
        return c.state === persistence.ChildState.Open;
      });
      let replicas = open_children.map((c) => {
        return new Replica({ uri: `bdev:///1?uuid=${c.uuid}` });
      });
      expect(replicas.length).greaterThan(1);

      let replicas_filtered = await persistent_store.filter_replicas("1", replicas);
      expect(replicas_filtered.length).equals(replicas.length);

      replicas_filtered.forEach((r) => {
        let child = open_children.find((c) => c.uuid === r.realUuid);
        expect(child).not.to.be.undefined;
        expect(child?.state).equals(persistence.ChildState.Open);
      });
    });
  });
};
