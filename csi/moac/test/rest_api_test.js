// Unit tests for the REST API

'use strict';

const expect = require('chai').expect;
const http = require('http');
const sinon = require('sinon');
const { Registry } = require('../dist/registry');
const { Node } = require('../dist/node');
const { GrpcError, grpcCode } = require('../dist/grpc_client');
const { ApiServer } = require('../dist/rest_api');

const PORT = 12312;
const STAT_COUNTER = 1000000; // feels good!
const UUID1 = '02de3df9-ce18-4164-89e1-b1cbf7a88e51';
const UUID2 = '02de3df9-ce18-4164-89e1-b1cbf7a88e52';
const UUID3 = '02de3df9-ce18-4164-89e1-b1cbf7a88e53';

module.exports = function () {
  let apiServer;
  let call1, call2, call3, call4;

  before(() => {
    const node1 = new Node('node1');
    const node2 = new Node('node2');
    const node3 = new Node('node3');
    const node4 = new Node('node4');
    const registry = new Registry({});
    registry.nodes = {
      node1,
      node2,
      node3,
      node4
    };
    call1 = sinon.stub(node1, 'call');
    call2 = sinon.stub(node2, 'call');
    call3 = sinon.stub(node3, 'call');
    call4 = sinon.stub(node4, 'call');
    call1.resolves({
      replicas: [
        {
          uuid: UUID1,
          pool: 'pool1',
          stats: {
            numReadOps: STAT_COUNTER,
            numWriteOps: STAT_COUNTER,
            bytesRead: STAT_COUNTER,
            bytesWritten: STAT_COUNTER
          }
        },
        {
          uuid: UUID2,
          pool: 'pool2',
          stats: {
            numReadOps: STAT_COUNTER,
            numWriteOps: STAT_COUNTER,
            bytesRead: STAT_COUNTER,
            bytesWritten: STAT_COUNTER
          }
        }
      ]
    });
    call2.rejects(new GrpcError(grpcCode.INTERNAL, 'test failure'));
    call3.resolves({
      replicas: [
        {
          uuid: UUID3,
          pool: 'pool3',
          stats: {
            numReadOps: STAT_COUNTER,
            numWriteOps: STAT_COUNTER,
            bytesRead: STAT_COUNTER,
            bytesWritten: STAT_COUNTER
          }
        }
      ]
    });
    call4.resolves({
      replicas: []
    });

    apiServer = new ApiServer(registry);
    apiServer.start(PORT);
  });

  after(() => {
    apiServer.stop();
  });

  it('should get ok for root url', (done) => {
    // TODO: Use user-friendly "request" lib when we have more tests
    http
      .get('http://127.0.0.1:' + PORT + '/', (resp) => {
        expect(resp.statusCode).to.equal(200);

        let data = '';
        resp.on('data', (chunk) => {
          data += chunk;
        });
        resp.on('end', () => {
          const obj = JSON.parse(data);
          expect(obj).to.deep.equal({});
          done();
        });
      })
      .on('error', done);
  });

  it('should get volume stats', (done) => {
    http
      .get('http://127.0.0.1:' + PORT + '/stats', (resp) => {
        expect(resp.statusCode).to.equal(200);

        let data = '';
        resp.on('data', (chunk) => {
          data += chunk;
        });
        resp.on('end', () => {
          const vols = JSON.parse(data);
          sinon.assert.calledOnce(call1);
          sinon.assert.calledWith(call1, 'statReplicas', {});
          sinon.assert.calledOnce(call2);
          sinon.assert.calledWith(call2, 'statReplicas', {});
          sinon.assert.calledOnce(call3);
          sinon.assert.calledWith(call3, 'statReplicas', {});
          sinon.assert.calledOnce(call4);
          sinon.assert.calledWith(call4, 'statReplicas', {});

          expect(vols).to.have.lengthOf(3);

          expect(vols[0].uuid).equal(UUID1);
          expect(vols[0].pool).equal('pool1');
          expect(vols[0].node).equal('node1');
          expect(vols[0].timestamp).to.be.a('string');
          // time delta between now and then is unlikely to be > 1s
          expect(new Date() - new Date(vols[0].timestamp)).to.be.below(1000);
          expect(vols[0].num_read_ops).equal(STAT_COUNTER);
          expect(vols[0].num_write_ops).equal(STAT_COUNTER);
          expect(vols[0].bytes_read).equal(STAT_COUNTER);
          expect(vols[0].bytes_written).equal(STAT_COUNTER);

          expect(vols[1].uuid).equal(UUID2);
          expect(vols[1].pool).equal('pool2');
          expect(vols[1].node).equal('node1');
          expect(vols[1].timestamp).to.be.a('string');
          // time delta between now and then is unlikely to be > 1s
          expect(new Date() - new Date(vols[1].timestamp)).to.be.below(1000);
          expect(vols[1].num_read_ops).equal(STAT_COUNTER);
          expect(vols[1].num_write_ops).equal(STAT_COUNTER);
          expect(vols[1].bytes_read).equal(STAT_COUNTER);
          expect(vols[1].bytes_written).equal(STAT_COUNTER);

          expect(vols[2].uuid).equal(UUID3);
          expect(vols[2].pool).equal('pool3');
          expect(vols[2].node).equal('node3');
          expect(vols[2].timestamp).to.be.a('string');
          // time delta between now and then is unlikely to be > 1s
          expect(new Date() - new Date(vols[2].timestamp)).to.be.below(1000);
          expect(vols[2].num_read_ops).equal(STAT_COUNTER);
          expect(vols[2].num_write_ops).equal(STAT_COUNTER);
          expect(vols[2].bytes_read).equal(STAT_COUNTER);
          expect(vols[2].bytes_written).equal(STAT_COUNTER);

          done();
        });
      })
      .on('error', done);
  });
};
