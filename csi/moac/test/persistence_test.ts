// Unit tests for the persistent store and its etcd client

import { Etcd3, IOptions, isRecoverableError } from 'etcd3';
import { defaults } from 'lodash';
import { Done } from 'mocha';
import { spawn, ChildProcessWithoutNullStreams } from 'child_process';
import { expect } from 'chai';
import { Replica } from '../src/replica';
import { Policy, ConsecutiveBreaker } from 'cockatiel';
import * as sinon from 'ts-sinon';
import { PersistentStore, NexusInfo, ChildInfo } from '../src/persistent_store';

const fs = require('fs');

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
  fs.rm(ETCD_STORE, { recursive: true }, (err: NodeJS.ErrnoException) => {
    if (err && err.code !== 'ENOENT') return done(err);

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
  });
}

// Kill etcd server. Though it does not wait for it to exit!
async function stopEtcd () {
  etcdProc?.kill();
  await fs.promises.rm(ETCD_STORE, { recursive: true });
}

module.exports = function () {
  // adds all possible combinations of child info to the NexusInfo
  // currently only healthy or otherwise
  function addChildrenCombinations(nexusInfo: NexusInfo) {
    const healthyChild = new ChildInfo({
      uuid: nexusInfo.children.length.toString(),
      healthy: true,
    });
    nexusInfo.children.push(healthyChild);
    const unhealthyChild = new ChildInfo({
      uuid: nexusInfo.children.length.toString(),
      healthy: false,
    });
    nexusInfo.children.push(unhealthyChild);
  }

  // returns a NexusInfo with the given clean_shutdown flag and twice all possible children combinations
  function getNexusInfo(cleanShutdown: boolean): NexusInfo {
    let nexusInfo = new NexusInfo({
      clean_shutdown: cleanShutdown,
      children: []
    });

    // add 2 of each
    addChildrenCombinations(nexusInfo);
    addChildrenCombinations(nexusInfo);

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

    after(stopEtcd);

    it('should read NexusInfo from the persistent store', async () => {
      let uuid = "1";

      let nexusNotThere = await client.get(uuid);
      expect(nexusNotThere).to.be.null;

      // now put it there
      // todo: use number format for the enums
      await client.put(uuid).value(JSON.stringify(getNexusInfo(true)));
      // and read it back
      let nexus = await client.get(uuid).json() as NexusInfo;

      expect(nexus).not.to.be.null;
      // inner values should match up
      expect(nexus.children.values).equals(getNexusInfo(true).children.values);
    });

    it('should throw if etcd is not reachable', async () => {
      const persistentStore = new PersistentStore([], 1000, () => client);

      await stopEtcd();
      let hasThrown = false;
      try {
        await persistentStore.filterReplicas("1", []);
      } catch (error: any) {
        hasThrown = true;
      }
      expect(hasThrown).to.be.true;

      // start etcd again
      await new Promise((resolve: (res: void) => void) => {
        startEtcd(() => {
          resolve();
        });
      });

      hasThrown = false;
      try {
        await persistentStore.filterReplicas("1", []);
      } catch (error: any) {
        console.log(`Caught unexpected exception, error: ${error}`);
        hasThrown = true;
      }
      expect(hasThrown).to.be.false;
    });

    it('should delete NexusInfo from the persistent store', async () => {
      let uuid = "1";

      let nexusNotThere = await client.get(uuid);
      expect(nexusNotThere).to.be.null;

      // now put it there
      await client.put(uuid).value(JSON.stringify(getNexusInfo(true)));
      // and read it back
      let nexus = await client.get(uuid).json() as NexusInfo;
      expect(nexus).not.to.be.null;

      const persistentStore = new PersistentStore([], 1000, () => client);
      await persistentStore.destroyNexus(uuid);

      nexusNotThere = await client.get(uuid);
      expect(nexusNotThere).to.be.null;
    });
  });

  describe('with mock etcd client', () => {
    const client = new Etcd3();
    const persistentStore = new PersistentStore([], 1000, () => client);

    it('should mock the persistent store', async () => {
      // hint: remove 'as any' cast to figure out which calls exec
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
      let hasThrown = false;
      try {
        await persistentStore.filterReplicas("1", replicas);
      } catch (error: any) {
        hasThrown = true;
      }
      expect(hasThrown).to.be.true;

      // valid json but not in the right format
      mock.exec.resolves({ kvs: [{ value: '{ "clean_shutdowns": true, "children": [] }' }] });
      hasThrown = false;
      try {
        await persistentStore.filterReplicas("1", replicas);
      } catch (error: any) {
        hasThrown = true;
      }
      expect(hasThrown).to.be.true;
      mock.exec.resolves({ kvs: [{ value: '{ "clean_shutdown": true, "childrens": [] }' }] });
      hasThrown = false;
      try {
        await persistentStore.filterReplicas("1", replicas);
      } catch (error: any) {
        hasThrown = true;
      }
      expect(hasThrown).to.be.true;

      // valid json and in the right format, so we should not throw now
      mock.exec.resolves({ kvs: [{ value: '{ "clean_shutdown": true, "children": [] }' }] });
      await persistentStore.filterReplicas("1", replicas);
    });

    it('should not filter out replicas on the first nexus creation', async () => {
      const mock = client.mock({ exec: sinon.default.stub() as any});
      mock.exec.resolves({ kvs: [] });
      let replicas = [new Replica({ uri: 'bdev:///1?uuid=1' }), new Replica({ uri: 'bdev:///1?uuid=2' })];
      let replicas_filtered = await persistentStore.filterReplicas("1", replicas);
      expect(replicas_filtered).equals(replicas);
    });

    it('should return a single healthy child on an unclean shutdown of the nexus', async () => {
      const mock = client.mock({ exec: sinon.default.stub() as any});
      let replicas = [new Replica({ uri: 'bdev:///1?uuid=1' }), new Replica({ uri: 'bdev:///1?uuid=2' })];

      // no children at all in the nexus, which is strange, but nonetheless, means we cannot create the nexus
      mock.exec.resolves({ kvs: [{ value: '{ "clean_shutdown": false, "children": [] }' }] });
      let replicasFiltered = await persistentStore.filterReplicas("1", replicas);
      expect(replicasFiltered.length).equals(0);

      let nexus = getNexusInfo(false);
      mock.exec.resolves({ kvs: [{ value: JSON.stringify(nexus) }] });
      let openChildren = nexus.children.filter((c) => {
        return c.healthy === true;
      });

      replicas = openChildren.map((c) => {
        return new Replica({ uri: `bdev:///1?uuid=${c.uuid}` });
      });
      expect(replicas.length).greaterThan(1);

      replicasFiltered = await persistentStore.filterReplicas("1", replicas);
      expect(replicasFiltered.length).equals(1);
      let child = openChildren.find((c) => replicasFiltered[0].realUuid === c.uuid);
      expect(child).not.to.be.undefined;
      expect(child?.healthy).to.be.true;
    });

    it('should return only healthy children on a clean shutdown of the nexus', async () => {
      const mock = client.mock({ exec: sinon.default.stub() as any});

      let nexus = getNexusInfo(true);
      mock.exec.resolves({ kvs: [{ value: JSON.stringify(nexus) }] });
      let openChildren = nexus.children.filter((c) => {
        return c.healthy === true;
      });
      let replicas = openChildren.map((c) => {
        return new Replica({ uri: `bdev:///1?uuid=${c.uuid}` });
      });
      expect(replicas.length).greaterThan(1);

      let replicasFiltered = await persistentStore.filterReplicas("1", replicas);
      expect(replicasFiltered.length).equals(replicas.length);

      replicasFiltered.forEach((r) => {
        let child = openChildren.find((c) => c.uuid === r.realUuid);
        expect(child).not.to.be.undefined;
        expect(child?.healthy).to.be.true;
      });
    });
  });
};
