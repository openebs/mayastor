#!/usr/bin/env node

// Main file of our control plane for mayastor.
// It binds all components together to create a meaningful whole.

'use strict';

const { KubeConfig } = require('client-node-fixed-watcher');
const yargs = require('yargs');
const logger = require('./logger');
const Registry = require('./registry');
const { NodeOperator } = require('./node_operator');
const { PoolOperator } = require('./pool_operator');
const { Volumes } = require('./volumes');
const { VolumeOperator } = require('./volume_operator');
const ApiServer = require('./rest_api');
const CsiServer = require('./csi').CsiServer;
const { MessageBus } = require('./nats');
const { PVHandler } = require('./pvhandler');

const log = new logger.Logger();

// Load k8s config file.
//
// @param   {string} [kubefile]    Kube config file.
// @returns {object}  k8s client object.
function createKubeConfig (kubefile) {
  const kubeConfig = new KubeConfig();
  try {
    if (kubefile) {
      log.info('Reading k8s configuration from file ' + kubefile);
      kubeConfig.loadFromFile(kubefile);
    } else {
      kubeConfig.loadFromDefault();
    }
  } catch (e) {
    log.error('Cannot get k8s client configuration: ' + e);
    process.exit(1);
  }
  return kubeConfig;
}

async function main () {
  let apiServer;
  let poolOper;
  let volumeOper;
  let csiNodeOper;
  let nodeOper;
  let kubeConfig;
  let warmupTimer;
  let pvhandler;

  const opts = yargs
    .options({
      a: {
        alias: 'csi-address',
        describe: 'Socket path where to listen for incoming CSI requests',
        default: '/var/tmp/csi.sock',
        string: true
      },
      i: {
        alias: 'heartbeat-interval',
        describe: 'Interval used by storage nodes for registration messages (seconds)',
        default: 5,
        number: true
      },
      k: {
        alias: 'kubeconfig',
        describe: 'Path to kubeconfig file',
        string: true
      },
      n: {
        alias: 'namespace',
        describe: 'Namespace of mayastor custom resources',
        default: 'default',
        string: true
      },
      m: {
        alias: 'message-bus',
        describe: 'NATS server endpoint in host[:port] form',
        default: '127.0.0.1:4222',
        string: true
      },
      p: {
        alias: 'port',
        describe: 'Port the REST API server should listen on',
        default: 3000,
        number: true
      },
      s: {
        alias: 'skip-k8s',
        describe:
          'Skip k8s client and k8s operators initialization (only for debug purpose)',
        default: false,
        boolean: true
      },
      v: {
        alias: 'verbose',
        describe: 'Print debug log messages',
        count: true
      },
      w: {
        alias: 'watcher-idle-timeout',
        describe: 'Restart watcher connections after this many seconds if idle',
        default: 0,
        number: true
      }
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
  async function cleanUp () {
    if (warmupTimer) clearTimeout(warmupTimer);
    if (csiServer) csiServer.undoReady();
    if (apiServer) apiServer.stop();
    if (!opts.s) {
      if (volumeOper) volumeOper.stop();
    }
    if (volumes) volumes.stop();
    if (!opts.s) {
      if (poolOper) poolOper.stop();
      if (csiNodeOper) await csiNodeOper.stop();
      if (nodeOper) nodeOper.stop();
    }
    if (messageBus) messageBus.stop();
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
  const csiServer = new CsiServer(opts.csiAddress);
  await csiServer.start();
  const registry = new Registry();

  // Listen to register and deregister messages from mayastor nodes
  const messageBus = new MessageBus(registry);
  messageBus.start(opts.m);

  if (!opts.s) {
    // Create k8s client and load openAPI spec from k8s api server
    kubeConfig = createKubeConfig(opts.kubeconfig);

    // Start k8s operators
    nodeOper = new NodeOperator(
      opts.namespace,
      kubeConfig,
      registry,
      opts.watcherIdleTimeout
    );
    await nodeOper.init(kubeConfig);
    await nodeOper.start();

    poolOper = new PoolOperator(
      opts.namespace,
      kubeConfig,
      registry,
      opts.watcherIdleTimeout
    );
    await poolOper.init(kubeConfig);
    await poolOper.start();
  }

  const volumes = new Volumes(registry);
  volumes.start();

  const warmupSecs = Math.floor(1.5 * opts.i);
  log.info(`Warming up will take ${warmupSecs} seconds ...`);
  warmupTimer = setTimeout(async () => {
    warmupTimer = undefined;
    if (!opts.s) {
      volumeOper = new VolumeOperator(
        opts.namespace,
        kubeConfig,
        volumes,
        opts.watcherIdleTimeout
      );
      await volumeOper.init(kubeConfig);
      await volumeOper.start();
    }

    apiServer = new ApiServer(registry);
    await apiServer.start(opts.port);

    csiServer.makeReady(registry, volumes);
    log.info('MOAC is warmed up and ready to 🚀');
  }, warmupSecs * 1000);

  pvhandler = new PVHandler(volumes)
  pvhandler.start()
}

main();
