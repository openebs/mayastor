#!/usr/bin/env node

// Script for reading stats data from moac REST API in periodic intervals
// and feeding those data to influx database.
//
// Assumptions:
// * "moac" hostname resolves to moac's IP address (moac = k8s service)
// * influxdb listens on loopback iface and uses standard port (8086)
// * influx credentials are passed in env variables

'use strict';

const Influx = require('influxdb-nodejs');
const request = require('request');
const yargs = require('yargs');
const logger = require('./logger');
const log = new logger.Logger();

const influxDb = process.env.INFLUXDB_DATABASE;
const influxUser = process.env.INFLUXDB_USERNAME;
const influxPassword = process.env.INFLUXDB_PASSWORD;

// if stats update operation is in progress
var inProgress = false;
var dbCreated = false;

// i --> integer
// s --> string
// f --> float
// b --> boolean
const fieldSchema = {
  num_read_ops: 'i',
  num_write_ops: 'i',
  bytes_read: 'i',
  bytes_written: 'i',
};
const tagSchema = {
  uuid: '*',
  pool: '*',
  node: '*',
};

function saveStats(client, stats, done) {
  var n = stats.length;

  stats.forEach(st => {
    log.trace('Saving stat ' + JSON.stringify(st) + ' to DB..');
    client
      .write('stats')
      .tag({
        uuid: st.uuid,
        pool: st.pool,
        node: st.node,
      })
      .field(st.stats)
      .then(() => {
        log.debug('Stat was successfully saved');
        if (--n == 0) done();
      })
      .catch(err => {
        log.error('Error saving stat to db: ' + err);
        if (--n == 0) done();
      });
  });
}

function pushStats(client, stats, done) {
  if (!dbCreated) {
    log.trace('Creating mayastor db..');
    client
      .createDatabase()
      .then(() => {
        saveStats(client, stats, done);
      })
      .catch(err => {
        done('Failed to create database: ' + err);
      });
  } else {
    saveStats(client, stats, done);
  }
}

function getStats(port, done) {
  const url = `http://moac:${port}/stats`;

  log.trace('Getting stats from ' + url);

  request(url, function(error, response, body) {
    if (error) {
      return done('Failed to get stats: ' + error);
    }
    if (!response || response.statusCode != 200) {
      return done('Invalid http status code');
    }
    var stats;
    try {
      stats = JSON.parse(body);
    } catch (err) {
      return done('Invalid JSON received: ' + body);
    }
    done(null, stats);
  });
}

function main() {
  let level = 'info';
  let opts = yargs
    .options({
      p: {
        alias: 'port',
        describe: 'Port of moac REST API server',
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

  if (!influxDb) {
    console.error('Error: INFLUXDB_DATABASE env variable must be set');
    process.exit(1);
  }
  if (!influxUser) {
    console.error('Error: INFLUXDB_USERNAME env variable must be set');
    process.exit(1);
  }
  if (!influxPassword) {
    console.error('Error: INFLUXDB_PASSWORD env variable must be set');
    process.exit(1);
  }

  if (opts.v) {
    if (opts.v == 1) level = 'debug';
    else level = 'silly';
  }
  logger.setLevel(level);

  const interval = opts._[0] ? parseInt(opts._[0]) : 10;
  const url = `http://${influxUser}:${influxPassword}@127.0.0.1:8086/${influxDb}`;
  const client = new Influx(url);

  client.schema('stats', fieldSchema, tagSchema, {
    stripUnknown: true, // default is false
  });

  setInterval(() => {
    if (inProgress) {
      log.warn('Previous stat has not finished - skipping this round');
      return;
    }
    inProgress = true;

    getStats(opts.port, (err, stats) => {
      if (err) {
        log.error(err.toString());
      } else if (stats.length == 0) {
        log.debug('No stats to save');
      } else {
        pushStats(client, stats, err => {
          if (err) {
            log.error(err.toString());
          }
          inProgress = false;
        });
        return;
      }
      inProgress = false;
    });
  }, 1000 * interval);
}

main();
