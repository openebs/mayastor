#!/usr/bin/env node

// Main file of our control plane for mayastor

'use strict';

const config = require('kubernetes-client').config;
const Client = require('kubernetes-client').Client;
const yargs = require('yargs');
const logger = require('./logger');
const { NodeOperator } = require('./nodes');
const { PoolOperator } = require('./pools');
const { VolumeOperator } = require('./volumes');
const { ApiServer } = require('./rest_api');
const CsiServer = require('./csi').CsiServer;

const log = new logger.Logger();

// Read k8s client configuration, in order to be able to connect to k8s api
// server, either from a file or from environment.
function readK8sConfig(kubeconfig) {
  try {
    if (kubeconfig != null) {
      log.info('Reading k8s configuration from file ' + kubeconfig);
      return config.fromKubeconfig(kubeconfig);
    } else {
      return config.getInCluster();
    }
  } catch (e) {
    log.error('Cannot get k8s client configuration: ' + e);
    process.exit(1);
  }
}

// Just print the list of nodes until we find better way how to use this
// information.
function printStatus(nodeOper, poolOper, volumeOper) {
  let nodes = nodeOper.get().map(n => n.node);
  log.info('List of storage nodes: ' + nodes.join(', '));

  let pools = poolOper.get().map(p => p.name + '@' + p.node);
  log.info('List of storage pools: ' + pools.join(', '));

  let repls = volumeOper
    .getReplica()
    .map(r => r.pool + '/' + r.uuid + '@' + r.node);
  log.info('List of replicas: ' + repls.join(', '));

  let nexus = volumeOper.getNexus().map(n => n.uuid + '@' + n.node);
  log.info('List of nexus: ' + nexus.join(', '));
}

async function main() {
  var volumeOper;
  var poolOper;
  var nodeOper;
  var csiServer;
  var apiServer;

  let opts = yargs
    .options({
      a: {
        alias: 'csi-address',
        describe: 'Socket path where to listen for incoming CSI requests',
        default: '/var/tmp/csi.sock',
        string: true,
      },
      k: {
        alias: 'kubeconfig',
        describe: 'Path to kubeconfig file',
        string: true,
      },
      p: {
        alias: 'port',
        describe: 'Port the REST API server should listen on',
        default: 3000,
        number: true,
      },
      v: {
        alias: 'verbose',
        describe: 'Print debug log messages',
        count: true,
      },
    })
    .help('help')
    .strict().argv;

  switch (opts.v) {
    case 0:
      logger.setLevel('info');
      break;
    case 1:
      logger.setLevel('debug');
      break;
    default:
      logger.setLevel('silly');
      break;
  }
  let k8sConfig = readK8sConfig(opts.kubeconfig);

  // We must install signal handlers before grpc lib does it.
  async function cleanUp() {
    csiServer.undoReady();
    if (apiServer) await apiServer.stop();
    if (volumeOper) await volumeOper.stop();
    if (poolOper) await poolOper.stop();
    if (nodeOper) await nodeOper.stop();
    if (csiServer) await csiServer.stop();
    process.exit(0);
  }
  process.on('SIGTERM', async () => {
    log.info('SIGTERM signal received.');
    await cleanUp();
  });
  process.on('SIGINT', async () => {
    log.info('SIGINT signal received.');
    await cleanUp();
  });

  // Create csi server before starting lengthy initialization so that we can
  // server csi.identity calls in the meantime.
  csiServer = new CsiServer(opts.csiAddress);
  await csiServer.start();

  // Create k8s client and load openAPI spec from k8s api server
  let client = new Client({ config: k8sConfig });
  log.debug('Loading openAPI spec from the server');
  await client.loadSpec();

  nodeOper = new NodeOperator();
  await nodeOper.init(client);

  poolOper = new PoolOperator();
  await poolOper.init(client, nodeOper);

  volumeOper = new VolumeOperator(nodeOper);
  apiServer = new ApiServer(volumeOper);

  await nodeOper.start();
  await apiServer.start(opts.port);
  await poolOper.start();
  await volumeOper.start();

  csiServer.makeReady(poolOper, volumeOper);

  // print node, pool & volume list when we start
  printStatus(nodeOper, poolOper, volumeOper);
}

main();
