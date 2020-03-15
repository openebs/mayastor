#!/usr/bin/env node

// Main file of our control plane for mayastor.
// It binds all components together to create a meaningful whole.

'use strict';

const { Client, KubeConfig } = require('kubernetes-client');
const Request = require('kubernetes-client/backends/request');
const yargs = require('yargs');
const logger = require('./logger');
const Registry = require('./registry');
const NodeOperator = require('./node_operator');
const PoolOperator = require('./pool_operator');
const Volumes = require('./volumes');
const ApiServer = require('./rest_api');
const CsiServer = require('./csi').CsiServer;

const log = new logger.Logger();

// Read k8s client configuration, in order to be able to connect to k8s api
// server, either from a file or from environment and return k8s client
// object.
//
// @param   {string} [kubefile]    Kube config file.
// @returns {object}  k8s client object.
function createK8sClient(kubefile) {
  var backend;
  try {
    if (kubefile != null) {
      log.info('Reading k8s configuration from file ' + kubefile);
      let kubeconfig = new KubeConfig();
      kubeconfig.loadFromFile(kubefile);
      backend = new Request({ kubeconfig });
    }
    return new Client({ backend });
  } catch (e) {
    log.error('Cannot get k8s client configuration: ' + e);
    process.exit(1);
  }
}

async function main() {
  var registry;
  var volumes;
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
      s: {
        alias: 'skip-k8s',
        describe:
          'Skip k8s client and k8s operators initialization (only for debug purpose)',
        default: false,
        boolean: true,
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

  // We must install signal handlers before grpc lib does it.
  async function cleanUp() {
    if (csiServer) csiServer.undoReady();
    if (apiServer) apiServer.stop();
    if (volumes) volumes.stop();
    if (!opts.s) {
      if (poolOper) await poolOper.stop();
      if (nodeOper) await nodeOper.stop();
    }
    if (registry) registry.close();
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
  // serve csi.identity() calls while getting ready.
  csiServer = new CsiServer(opts.csiAddress);
  await csiServer.start();
  registry = new Registry();

  if (!opts.s) {
    // Create k8s client and load openAPI spec from k8s api server
    let client = createK8sClient(opts.kubeconfig);
    log.debug('Loading openAPI spec from the server');
    await client.loadSpec();

    // Start k8s operators
    nodeOper = new NodeOperator();
    await nodeOper.init(client, registry);
    await nodeOper.start();

    poolOper = new PoolOperator();
    await poolOper.init(client, registry);
    await poolOper.start();
  }

  volumes = new Volumes(registry);
  volumes.start();

  apiServer = new ApiServer(registry);
  await apiServer.start(opts.port);

  csiServer.makeReady(registry, volumes);
  log.info('MOAC is up and ready 🚀');
}

main();
