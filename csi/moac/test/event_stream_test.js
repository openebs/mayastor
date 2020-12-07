// Unit tests for event stream

'use strict';

const expect = require('chai').expect;
const EventEmitter = require('events');
const sinon = require('sinon');
const { Pool } = require('../pool');
const { Replica } = require('../replica');
const { Nexus } = require('../nexus');
const Registry = require('../registry');
const { Volume } = require('../volume');
const { Volumes } = require('../volumes');
const EventStream = require('../event_stream');

module.exports = function () {
  // Easy generator of a test node with fake pools, replicas and nexus
  // omitting all properties that are not necessary for the event stream.
  class FakeNode {
    constructor (name, pools, nexus) {
      this.name = name;
      this.pools = pools.map((obj) => {
        const p = new Pool({ name: obj.name, disks: ['/dev/sda'] });
        p.node = new EventEmitter();
        obj.replicas.forEach((uuid) =>
          p.registerReplica(new Replica({ uuid }))
        );
        return p;
      });
      this.nexus = nexus.map((uuid) => new Nexus({ uuid, children: [] }));
    }
  }

  it('should read events from registry and volumes stream', (done) => {
    const registry = new Registry();
    const volumes = new Volumes(registry);
    const getNodeStub = sinon.stub(registry, 'getNode');
    const getVolumeStub = sinon.stub(volumes, 'list');
    // The initial state of the nodes. "new" event should be written to the
    // stream for all these objects and one "sync" event for each node meaning
    // that the reader has caught up with the initial state.
    getNodeStub.returns([
      new FakeNode(
        'node1',
        [
          {
            name: 'pool1',
            replicas: ['uuid1', 'uuid2']
          },
          {
            name: 'pool2',
            replicas: ['uuid3']
          }
        ],
        ['nexus1', 'nexus2']
      ),
      new FakeNode(
        'node2',
        [
          {
            name: 'pool3',
            replicas: ['uuid4', 'uuid5', 'uuid6']
          }
        ],
        []
      )
    ]);
    getVolumeStub.returns([
      new Volume('volume1', registry, () => {}, {}),
      new Volume('volume2', registry, () => {}, {})
    ]);

    // set low high water mark to test buffered reads
    const stream = new EventStream(
      {
        registry,
        volumes
      },
      {
        highWaterMark: 3,
        lowWaterMark: 1
      }
    );
    const events = [];

    stream.on('data', (ev) => {
      events.push(ev);
    });

    setTimeout(() => {
      registry.emit('pool', {
        eventType: 'new',
        object: { name: 'pool4' }
      });
      registry.emit('pool', {
        eventType: 'mod',
        object: { name: 'pool3' }
      });
      registry.emit('pool', {
        eventType: 'del',
        object: { name: 'pool4' }
      });

      setTimeout(() => {
        // exhibit buffering
        stream.pause();

        registry.emit('node', {
          eventType: 'sync',
          object: { name: 'node3' }
        });

        registry.emit('replica', {
          eventType: 'new',
          object: { uuid: 'replica1' }
        });
        registry.emit('replica', {
          eventType: 'mod',
          object: { uuid: 'replica2' }
        });
        registry.emit('replica', {
          eventType: 'del',
          object: { uuid: 'replica3' }
        });

        registry.emit('nexus', {
          eventType: 'new',
          object: { uuid: 'nexus1' }
        });
        registry.emit('nexus', {
          eventType: 'mod',
          object: { uuid: 'nexus2' }
        });
        registry.emit('nexus', {
          eventType: 'del',
          object: { uuid: 'nexus3' }
        });

        volumes.emit('volume', {
          eventType: 'new',
          object: { uuid: 'volume3' }
        });
        volumes.emit('volume', {
          eventType: 'mod',
          object: { uuid: 'volume4' }
        });
        volumes.emit('volume', {
          eventType: 'del',
          object: { uuid: 'volume5' }
        });

        registry.emit('unknown', {
          eventType: 'new',
          object: { name: 'something' }
        });

        stream.resume();

        setTimeout(() => {
          stream.destroy();
        }, 1);
      }, 1);
    }, 1);

    stream.once('end', () => {
      let i = 0;
      // A note about ordering of events that are part of the initial state:
      // First go pools. Each pool is followed by its replicas. Nexus go last.
      // Then follow volume "new" events.
      expect(events).to.have.lengthOf.at.least(30);
      expect(events[i].kind).to.equal('node');
      expect(events[i].eventType).to.equal('new');
      expect(events[i++].object.name).to.equal('node1');
      expect(events[i].kind).to.equal('pool');
      expect(events[i].eventType).to.equal('new');
      expect(events[i++].object.name).to.equal('pool1');
      expect(events[i].kind).to.equal('replica');
      expect(events[i].eventType).to.equal('new');
      expect(events[i++].object.uuid).to.equal('uuid1');
      expect(events[i].kind).to.equal('replica');
      expect(events[i].eventType).to.equal('new');
      expect(events[i++].object.uuid).to.equal('uuid2');
      expect(events[i].kind).to.equal('pool');
      expect(events[i].eventType).to.equal('new');
      expect(events[i++].object.name).to.equal('pool2');
      expect(events[i].kind).to.equal('replica');
      expect(events[i].eventType).to.equal('new');
      expect(events[i++].object.uuid).to.equal('uuid3');
      expect(events[i].kind).to.equal('nexus');
      expect(events[i].eventType).to.equal('new');
      expect(events[i++].object.uuid).to.equal('nexus1');
      expect(events[i].kind).to.equal('nexus');
      expect(events[i].eventType).to.equal('new');
      expect(events[i++].object.uuid).to.equal('nexus2');
      expect(events[i].kind).to.equal('node');
      expect(events[i].eventType).to.equal('sync');
      expect(events[i++].object.name).to.equal('node1');
      expect(events[i].kind).to.equal('node');
      expect(events[i].eventType).to.equal('new');
      expect(events[i++].object.name).to.equal('node2');
      expect(events[i].kind).to.equal('pool');
      expect(events[i].eventType).to.equal('new');
      expect(events[i++].object.name).to.equal('pool3');
      expect(events[i].kind).to.equal('replica');
      expect(events[i].eventType).to.equal('new');
      expect(events[i++].object.uuid).to.equal('uuid4');
      expect(events[i].kind).to.equal('replica');
      expect(events[i].eventType).to.equal('new');
      expect(events[i++].object.uuid).to.equal('uuid5');
      expect(events[i].kind).to.equal('replica');
      expect(events[i].eventType).to.equal('new');
      expect(events[i++].object.uuid).to.equal('uuid6');
      expect(events[i].kind).to.equal('node');
      expect(events[i].eventType).to.equal('sync');
      expect(events[i++].object.name).to.equal('node2');
      expect(events[i].kind).to.equal('volume');
      expect(events[i].eventType).to.equal('new');
      expect(events[i++].object.uuid).to.equal('volume1');
      expect(events[i].kind).to.equal('volume');
      expect(events[i].eventType).to.equal('new');
      expect(events[i++].object.uuid).to.equal('volume2');
      // these events happened after the stream was created
      expect(events[i].kind).to.equal('pool');
      expect(events[i].eventType).to.equal('new');
      expect(events[i++].object.name).to.equal('pool4');
      expect(events[i].kind).to.equal('pool');
      expect(events[i].eventType).to.equal('mod');
      expect(events[i++].object.name).to.equal('pool3');
      expect(events[i].kind).to.equal('pool');
      expect(events[i].eventType).to.equal('del');
      expect(events[i++].object.name).to.equal('pool4');
      expect(events[i].kind).to.equal('node');
      expect(events[i].eventType).to.equal('sync');
      expect(events[i++].object.name).to.equal('node3');
      expect(events[i].kind).to.equal('replica');
      expect(events[i].eventType).to.equal('new');
      expect(events[i++].object.uuid).to.equal('replica1');
      expect(events[i].kind).to.equal('replica');
      expect(events[i].eventType).to.equal('mod');
      expect(events[i++].object.uuid).to.equal('replica2');
      expect(events[i].kind).to.equal('replica');
      expect(events[i].eventType).to.equal('del');
      expect(events[i++].object.uuid).to.equal('replica3');
      expect(events[i].kind).to.equal('nexus');
      expect(events[i].eventType).to.equal('new');
      expect(events[i++].object.uuid).to.equal('nexus1');
      expect(events[i].kind).to.equal('nexus');
      expect(events[i].eventType).to.equal('mod');
      expect(events[i++].object.uuid).to.equal('nexus2');
      expect(events[i].kind).to.equal('nexus');
      expect(events[i].eventType).to.equal('del');
      expect(events[i++].object.uuid).to.equal('nexus3');
      expect(events[i].kind).to.equal('volume');
      expect(events[i].eventType).to.equal('new');
      expect(events[i++].object.uuid).to.equal('volume3');
      expect(events[i].kind).to.equal('volume');
      expect(events[i].eventType).to.equal('mod');
      expect(events[i++].object.uuid).to.equal('volume4');
      expect(events[i].kind).to.equal('volume');
      expect(events[i].eventType).to.equal('del');
      expect(events[i++].object.uuid).to.equal('volume5');
      expect(events).to.have.lengthOf(i);
      done();
    });
  });
};
