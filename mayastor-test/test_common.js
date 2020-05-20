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
const sudo = require('./sudo');

const SOCK = '/tmp/mayastor_test.sock';
const MS_CONFIG_PATH = '/tmp/mayastor_test.cfg';
const SPDK_CONFIG_PATH = '/tmp/spdk_test.cfg';
const GRPC_PORT = 10124;
const CSI_ENDPOINT = '/tmp/mayastor_csi_test.sock';
const CSI_ID = 'test-node-id';
const LOCALHOST = '127.0.0.1';

var testPort = process.env.TEST_PORT || GRPC_PORT;
var myIp = getMyIp() || LOCALHOST;
var grpcEndpoint = myIp + ':' + testPort;
// started processes indexed by the program name
var procs = {};

// Construct path to a rust binary in target/debug/... dir.
function getCmdPath (name) {
  return path.join(__dirname, '..', 'target', 'debug', name);
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

// Common code for starting mayastor, mayastor-grpc and spdk processes.
function startProcess (command, args, env, closeCb, psName) {
  assert(!procs[command]);
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
    delete procs[command];
    if (closeCb) closeCb();
  });
  procs[command] = proc;
}

// Start spdk process and return immediately.
function startSpdk (config, args, env) {
  args = args || ['-r', SOCK];
  env = env || {};

  if (config) {
    fs.writeFileSync(SPDK_CONFIG_PATH, config);
    args = args.concat(['-c', SPDK_CONFIG_PATH]);
  }

  startProcess(
    'spdk',
    args,
    _.assign(
      {
        MAYASTOR_DELAY: '1'
      },
      env
    ),
    () => {
      try {
        fs.unlinkSync(SPDK_CONFIG_PATH);
      } catch (err) {}
    },
    'reactor_0'
  );
}

// Start mayastor process and return immediately.
function startMayastor (config, args, env) {
  args = args || ['-r', SOCK];
  env = env || {};

  if (config) {
    fs.writeFileSync(MS_CONFIG_PATH, config);
    args = args.concat(['-c', MS_CONFIG_PATH]);
  }

  startProcess(
    'mayastor',
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
        fs.unlinkSync(MS_CONFIG_PATH);
      } catch (err) {}
    },
    'mayastor'
  );
}

// Start mayastor-agent processes and return immediately.
function startMayastorGrpc () {
  startProcess('mayastor-agent', [
    '-v',
    '-n',
    'test-node-id',
    '-a',
    getMyIp(),
    '-p',
    GRPC_PORT.toString(),
    '-c',
    CSI_ENDPOINT,
    '-s',
    SOCK
  ]);
}

function killSudoedProcess (name, pid, done) {
  find('name', name).then((res) => {
    var whichPid;
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
      killSudoedProcess(name, proc.pid, (err) => {
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
  const proc = procs.mayastor;
  assert(proc);

  async.series(
    [
      (next) => {
        killSudoedProcess('mayastor', proc.pid, (err) => {
          if (err) return next(err);
          if (procs.mayastor) {
            procs.mayastor.once('close', next);
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

// Restart mayastor-agent process.
//
// TODO: We don't restart the process with the same parameters as we
// don't remember params which were used for starting it.
function restartMayastorGrpc (ping, done) {
  const proc = procs['mayastor-agent'];
  assert(proc);

  async.series(
    [
      (next) => {
        killSudoedProcess('mayastor-agent', proc.pid, (err) => {
          if (err) return next(err);
          if (procs['mayastor-agent']) {
            procs['mayastor-agent'].once('close', next);
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
        startMayastorGrpc();
        waitFor(ping, next);
      }
    ],
    done
  );
}

// Execute rpc method using dumb jsonrpc client
function dumbCommand (method, args, done) {
  exec(
    '../target/debug/mctl -s ' +
      SOCK +
      ' raw' +
      ' ' +
      method +
      " '" +
      JSON.stringify(args) +
      "'",
    (err, stdout, stderr) => {
      done(err, stdout);
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
  startSpdk,
  startMayastor,
  startMayastorGrpc,
  stopAll,
  waitFor,
  restartMayastor,
  restartMayastorGrpc,
  fixSocketPerms,
  grpcEndpoint,
  dumbCommand,
  execAsRoot,
  runAsRoot,
  ensureNbdWritable,
  restoreNbdPerms,
  getMyIp,
  getCmdPath
};
