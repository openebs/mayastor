// Main file of our control plane for mayastor.
// It binds all components together to create a meaningful whole.

const { KubeConfig } = require('@kubernetes/client-node');
const yargs = require('yargs');


import * as fs from 'fs';
import { NodeOperator } from './node_operator';
import { PoolOperator } from './pool_operator';
import { Registry } from './registry';
import { ApiServer } from './rest_api';
import { MessageBus } from './nats';
import { Volumes } from './volumes';
import { VolumeOperator } from './volume_operator';
import { CsiServer } from './csi';
import { PersistentStore } from './persistent_store';
import * as logger from './logger';

const log = logger.Logger();

const NAMESPACE_FILE = '/var/run/secrets/kubernetes.io/serviceaccount/namespace';

// Load k8s config file.
//
// @param   {string} [kubefile]    Kube config file.
// @returns {object}  k8s client object.
function createKubeConfig (kubefile: string): any {
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

export async function main () {
  let apiServer: any;
  let poolOper: PoolOperator;
  let volumeOper: VolumeOperator;
  let nodeOper: NodeOperator;
  let kubeConfig: any;
  let warmupTimer: NodeJS.Timeout | undefined;

  const opts = yargs
    .options({
      a: {
        alias: 'csi-address',
        describe: 'Socket path where to listen for incoming CSI requests',
        default: '/var/tmp/csi.sock',
        string: true
      },
      e: {
        alias: 'etcd-endpoint',
        describe: 'ETCD endpoint in host[:port] form',
        default: '127.0.0.1:2379',
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
        describe: 'Override default namespace of mayastor custom resources',
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
      'sync-period': {
        describe: 'Sync period for a storage node that is known to be healthy (in seconds)',
        default: 60,
        number: true
      },
      'sync-retry': {
        describe: 'Sync period for a storage nodes that is known to be bad (in seconds)',
        default: 10,
        number: true
      },
      'sync-bad-limit': {
        describe: 'Storage node moves to offline state after this many retries (0 means immediately when it fails)',
        default: 0,
        number: true
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

  // Determine the namespace that should be used for CRDs
  let namespace: string = 'default';
  if (opts.namespace) {
    namespace = opts.namespace;
  } else if (!opts.s) {
    try {
      namespace = fs.readFileSync(NAMESPACE_FILE).toString();
    } catch (err) {
      log.error(`Cannot read pod namespace from ${NAMESPACE_FILE}: ${err}`);
      process.exit(1);
    }
  }
  log.debug(`Operating in namespace "${namespace}"`);


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
  const persistentStore = new PersistentStore([opts.e]);

  await csiServer.start();
  let registry = new Registry({
    syncPeriod: opts.syncPeriod * 1000,
    syncRetry: opts.syncRetry * 1000,
    syncBadLimit: opts.syncBadLimit,
  }, persistentStore);

  // Listen to register and deregister messages from mayastor nodes
  const messageBus = new MessageBus(registry);
  messageBus.start(opts.m);

  if (!opts.s) {
    // Create k8s client and load openAPI spec from k8s api server
    kubeConfig = createKubeConfig(opts.kubeconfig);

    // Start k8s operators
    nodeOper = new NodeOperator(
      namespace,
      kubeConfig,
      registry,
      opts.watcherIdleTimeout
    );
    await nodeOper.init(kubeConfig);
    await nodeOper.start();

    poolOper = new PoolOperator(
      namespace,
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
        namespace,
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
}
