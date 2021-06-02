#!/usr/bin/env node

// Message bus client
//
// A simple program for publishing messages to the NATS server for purpose
// of debugging mayastor.

'use strict';

const yargs = require('yargs');
const nats = require('nats');

const opts = yargs
  .options({
    s: {
      alias: 'server',
      describe: 'NATS server address in host[:port] form',
      default: '127.0.0.1:4222',
      string: true
    }
  })
  .command('register <node> <grpc>', 'Send registration request', (yargs) => {
    yargs.positional('node', {
      describe: 'Node name',
      type: 'string'
    });
    yargs.positional('grpc', {
      describe: 'gRPC endpoint',
      type: 'string'
    });
  })
  .command('deregister <node>', 'Send deregistration request', (yargs) => {
    yargs.positional('node', {
      describe: 'Node name',
      type: 'string'
    });
  })
  .command('raw <name> <payload>', 'Publish raw NATS message', (yargs) => {
    yargs.positional('name', {
      describe: 'Name of the message',
      type: 'string'
    });
    yargs.positional('payload', {
      describe: 'Raw payload sent as a string',
      type: 'string'
    });
  })
  .help('help')
  .strict().argv;

const nc = nats.connect(opts.s);
nc.on('connect', () => {
  if (opts._[0] === 'register') {
    nc.publish('v0/registry', JSON.stringify({
      id: 'v0/register',
      sender: 'moac',
      data: {
        id: opts.node,
        grpcEndpoint: opts.grpc
      }
    }));
  } else if (opts._[0] === 'deregister') {
    nc.publish('v0/registry', JSON.stringify({
      id: 'v0/deregister',
      sender: 'moac',
      data: {
        id: opts.node
      }
    }));
  } else if (opts._[0] === 'raw') {
    nc.publish(opts.name, opts.payload);
  }
  nc.flush();
  nc.close();
  process.exit(0);
});
nc.on('error', (err) => {
  console.error(err.toString());
  nc.close();
  process.exit(1);
});
