// Common utility functions shared by grpc tests.

'use strict';

const _ = require('lodash');
const assert = require('assert');
const async = require('async');
const find = require('find-process');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { exec, spawn } = require('child_process');
const { createClient } = require('grpc-kit');
const sudo = require('./sudo');

const SOCK = '/tmp/mayastor_test.sock';
const MS_CONFIG_PATH = '/tmp/mayastor_test.cfg';
const GRPC_PORT = 10124;
const CSI_ENDPOINT = '/tmp/mayastor_csi_test.sock';
const CSI_ID = 'test-node-id';
const LOCALHOST = '127.0.0.1';

const testPort = process.env.TEST_PORT || GRPC_PORT;
const myIp = getMyIp() || LOCALHOST;
const grpcEndpoint = myIp + ':' + testPort;
// started processes indexed by the program name
let procs = {};

// Construct path to a rust binary in target/debug/... dir.
function getCmdPath (name) {
  return path.join(__dirname, '..', '..', 'target', 'debug', name);
}

// Run the command as root. We use sudo to gain root privileges.
// If already running with euid = 0, then just spawn the command.
// Return child process handle.
//
// TODO: Beware that glob expansion of file names works differently
// between the two cases. When using just spawn() file names are not
// expanded.
function runAsRoot (cmd, args, env, nameInPs) {
  env = env || {};
  env = _.assignIn(
    {},
    process.env,
    {
      RUST_BACKTRACE: 1
    },
    env
  );
  if (process.geteuid() === 0) {
    return spawn(cmd, args || [], {
      env,
      shell: '/bin/bash'
    });
  } else {
    return sudo(
      [cmd].concat(args || []),
      {
        spawnOptions: { env }
      },
      nameInPs
    );
  }
}

// Execute command as root and call callback with (error, stdout) arguments
// when the command has finished.
function execAsRoot (cmd, args, done) {
  const child = runAsRoot(cmd, args);
  let stderr = '';
  let stdout = '';

  child.stderr.on('data', (data) => {
    stderr += data;
  });
  child.stdout.on('data', (data) => {
    stdout += data;
  });
  child.on('close', (code, signal) => {
    if (code !== 0) {
      done(
        new Error(
          `Command ${cmd} exited with code ${code}. Error output: ${stderr}`
        )
      );
    } else if (signal) {
      done(
        new Error(
          `Command ${cmd} terminated by signal ${signal}. Error output: ${stderr}`
        )
      );
    } else {
      done(null, stdout);
    }
  });
}

// Periodically ping mayastor until up and running.
// Ping cb with grpc call is provided by the caller.
function waitFor (ping, done) {
  let lastError;
  let iters = 0;

  async.whilst(
    (cb) => {
      cb(null, iters < 10);
    },
    (next) => {
      iters++;
      ping((err) => {
        if (err) {
          lastError = err;
          setTimeout(next, 1000);
        } else {
          lastError = undefined;
          iters = 10;
          next();
        }
      });
    },
    () => {
      done(lastError);
    }
  );
}

// Find the first usable external IPv4 address on the system
function getMyIp () {
  const externIp = _.map(
    _.flatten(Object.values(os.networkInterfaces())),
    'address'
  ).find((addr) => addr.indexOf(':') < 0 && !addr.match(/^127\./));
  assert(externIp, 'Cannot determine external IP address of the system');
  return externIp;
}

// Common code for starting mayastor, mayastor-csi and spdk processes.
function startProcess (command, args, env, closeCb, psName, suffix) {
  let procsIndex = command;
  if (suffix) {
    procsIndex += suffix;
  }
  assert(!procs[procsIndex]);
  const proc = runAsRoot(getCmdPath(command), args, env, psName);
  proc.output = [];

  proc.stdout.on('data', (data) => {
    proc.output.push(data);
  });
  proc.stderr.on('data', (data) => {
    proc.output.push(data);
  });
  proc.once('close', (code, signal) => {
    console.log(`${command} exited with code=${code} and signal=${signal}:`);
    console.log('-----------------------------------------------------');
    console.log(proc.output.join('').trim());
    console.log('-----------------------------------------------------');
    delete procs[procsIndex];
    if (closeCb) closeCb();
  });
  procs[procsIndex] = proc;
}

// Start mayastor process and return immediately.
function startMayastor (config, args, env, suffix) {
  args = args || ['-r', SOCK, '-g', grpcEndpoint];
  env = env || {};
  let configPath = MS_CONFIG_PATH;
  if (suffix) {
    configPath += suffix;
  }

  if (config) {
    fs.writeFileSync(configPath, config);
    args = args.concat(['-y', configPath]);
  }

  startProcess(
    'io-engine',
    args,
    _.assign(
      {
        MY_POD_IP: getMyIp(),
        MAYASTOR_DELAY: '1'
      },
      env
    ),
    () => {
      try {
        fs.unlinkSync(configPath);
      } catch (err) { }
    },
    'io-engine',
    suffix
  );
}

function killSudoedProcess (name, pid, done) {
  find('name', name).then((res) => {
    let whichPid;
    if (process.geteuid() === 0) {
      whichPid = 'pid';
    } else {
      whichPid = 'ppid';
    }
    res = res.filter((ent) => ent[whichPid] === pid);
    if (res.length === 0) {
      return done();
    }
    const child = runAsRoot('kill', ['-s', 'SIGTERM', res[0].pid.toString()]);
    child.stderr.on('data', (data) => {
      console.log('kill', name, 'error:', data.toString());
    });
    child.once('close', () => {
      done();
    });
  });
}

// Kill all previously started processes.
function stopAll (done) {
  // Unfortunately the order in which the procs are stopped matters (hence the
  // sort()). In nexus tests if spdk proc with connected nvmf target is stopped
  // before nvmf initiator in mayastor, it exits with segfault. That's also the
  // reason why we use mapSeries instead of parallel map.
  async.mapSeries(
    Object.keys(procs).sort(),
    (name, cb) => {
      const proc = procs[name];
      console.log(`Stopping ${name} with pid ${proc.pid} ...`);
      killSudoedProcess(proc.nameInPs, proc.pid, (err) => {
        if (err) return cb(null, err);
        // let other close event handlers on the process run
        setTimeout(cb, 0);
      });
    },
    (err, errors) => {
      assert(!err);
      procs = {};
      // return the first found error
      done(errors.find((e) => !!e));
    }
  );
}

// Restart mayastor process.
//
// TODO: We don't restart the mayastor with the same parameters as we
// don't remember params which were used for starting it.
function restartMayastor (ping, done) {
  const proc = procs.io_engine;
  assert(proc);

  async.series(
    [
      (next) => {
        killSudoedProcess('io-engine', proc.pid, (err) => {
          if (err) return next(err);
          if (procs.io_engine) {
            procs.io_engine.once('close', next);
          } else {
            next();
          }
        });
      },
      (next) => {
        // let other close event handlers on the process run
        setTimeout(next, 0);
      },
      (next) => {
        startMayastor();
        waitFor(ping, next);
      }
    ],
    done
  );
}

// Execute rpc method using jsonrpc client
function jsonrpcCommand (sock, method, args, done) {
  if (!sock) sock = SOCK;
  if (!done) {
    done = args;
    args = null;
  }
  let cmd = getCmdPath('jsonrpc') + ' -s ' + sock + ' raw' + ' ' + method;
  if (args !== null && args !== undefined) {
    cmd += " '" + JSON.stringify(args) + "'";
  }
  exec(cmd, (err, stdout, stderr) => {
    if (err) {
      done(new Error(stderr));
    } else {
      done(err, stdout);
    }
  });
}

// Create mayastor grpc client. Must be closed by the user when not used anymore.
function createGrpcClient (serviceName = 'Mayastor', endpoint = grpcEndpoint) {
  const client = createClient(
    {
      protoPath: path.join(
        __dirname,
        '..',
        '..',
        'rpc',
        'mayastor-api',
        'protobuf',
        'mayastor.proto'
      ),
      packageName: 'mayastor',
      serviceName: serviceName,
      options: {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true
      }
    },
    endpoint
  );
  if (!client) {
    throw new Error('Failed to initialize grpc client');
  }
  return client;
}

// Create mayastor grpc client, call a method and return the result of it.
function callGrpcMethod (method, args, done) {
  let client;
  try {
    client = createGrpcClient();
  } catch (err) {
    return done(err);
  }
  client[method](args, (err, res) => {
    client.close();
    done(err, res);
  });
}

// Create bdevrpc grpc client, create the specified bdevs,
// and export them via the specified protocol.
// Close the client when we are done.
function createBdevs (bdevs, proto, endpoint, done) {
  let client;
  try {
    client = createGrpcClient('BdevRpc', endpoint);
  } catch (err) {
    return done(err);
  }
  async.each(
    bdevs,
    (uri, next) => {
      client.create(
        { uri: uri },
        (err, res) => {
          if (err) return next(err);
          client.share({ name: res.name, proto: proto }, next);
        }
      );
    },
    (err, res) => {
      client.close();
      done(err, res);
    }
  );
}

// Ensure that /dev/nbd* devices are writable by the current process.
// If running as root this is a noop.
function ensureNbdWritable (done) {
  if (process.geteuid() !== 0) {
    const child = runAsRoot('sh', ['-c', 'chmod o+rw /dev/nbd*']);
    child.stderr.on('data', (data) => {
      console.log(data.toString());
    });
    child.on('close', (code, signal) => {
      if (code !== 0) {
        done(new Error('Failed to chmod nbd devs'));
      } else {
        done();
      }
    });
  } else {
    done();
  }
}

// Unix domain socket client does not run with root privs (in general) so open
// the socket to everyone.
function fixSocketPerms (done) {
  const child = runAsRoot('chmod', ['a+rw', CSI_ENDPOINT]);
  child.stderr.on('data', (data) => {
    // console.log('chmod', 'error:', data.toString());
  });
  child.on('close', (code) => {
    if (code !== 0) {
      done('Failed to chmod the socket' + code);
    } else {
      done();
    }
  });
}

// Undo change to perms of nbd devices done in ensureNbdWritable().
function restoreNbdPerms (done) {
  if (process.geteuid() !== 0) {
    const child = runAsRoot('sh', ['-c', 'chmod o-rw /dev/nbd*']);
    child.on('close', (code, signal) => {
      if (code !== 0) {
        done(new Error('Failed to chmod nbd devs'));
      } else {
        done();
      }
    });
  } else {
    done();
  }
}

module.exports = {
  CSI_ENDPOINT,
  CSI_ID,
  SOCK,
  startMayastor,
  stopAll,
  waitFor,
  restartMayastor,
  fixSocketPerms,
  grpcEndpoint,
  jsonrpcCommand,
  execAsRoot,
  runAsRoot,
  ensureNbdWritable,
  restoreNbdPerms,
  getMyIp,
  getCmdPath,
  createGrpcClient,
  callGrpcMethod,
  createBdevs
};
