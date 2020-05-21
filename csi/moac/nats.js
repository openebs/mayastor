// Interface to the NATS server where mayastor instances send registration
// requests and events.

'use strict';

const assert = require('assert');
const nats = require('nats');
const log = require('./logger').Logger('nats');

// If NATS server is not available then keep trying to connect in this interval
const RECONNECT_DELAY = 10000; // in ms

// Message bus object subscribes to messages from NATS server and handles each
// message by dispatching it further to other moac components.
class MessageBus {
  // Create a new message bus object.
  //
  // @param {object} registry        Object registry used for adding/removing of nodes.
  // @param {object} reconnectDelay  If NATS srv is unavailable, keep trying with this delay.
  constructor (registry, reconnectDelay) {
    assert(registry);
    this.registry = registry;
    this.endpoint = '';
    this.nc = null;
    this.timeout = null;
    this.connected = false;
    this.reconnectDelay = reconnectDelay || RECONNECT_DELAY;
  }

  // Connect to the NATS server
  //
  // @param {string} endpoint   NATS server's address and port.
  start (endpoint) {
    assert(!this.nc);
    this.endpoint = endpoint;
    this._connect();
  }

  // Disconnect from the NATS server
  stop () {
    if (this.timeout) clearTimeout(this.timeout);
    this._disconnect();
  }

  // Return if the bus is connected to the NATS server.
  //
  // @returns {boolean} true if connected otherwise false.
  isConnected () {
    return this.connected;
  }

  _connect () {
    log.debug(`Connecting to NATS at "${this.endpoint}" ...`);
    if (this.timeout) clearTimeout(this.timeout);
    assert(!this.nc);
    this.nc = nats.connect({
      servers: [`nats://${this.endpoint}`]
    });
    var self = this;
    this.nc.on('connect', () => {
      log.info(`Connected to NATS message bus at "${this.endpoint}"`);
      self.connected = true;
      self._subscribe();
    });
    this.nc.on('error', (err) => {
      log.error(`${err}`);
      self._disconnect();
      log.debug(`Reconnecting after ${self.reconnectDelay}ms`);
      // reconnect but give it some time to recover to prevent spinning in loop
      self.timeout = setTimeout(self._connect.bind(self), self.reconnectDelay);
    });
  }

  _disconnect () {
    if (this.nc) {
      this.nc.close();
      this.nc = null;
      this.connected = false;
      log.info('Disconnected from NATS message bus');
    }
  }

  _parsePayload (msg) {
    if (typeof (msg.data) !== 'string') {
      log.error(`Invalid payload in ${msg.subject} message: not a string`);
      return;
    }
    try {
      return JSON.parse(msg.data);
    } catch (e) {
      log.error(`Invalid payload in ${msg.subject} message: not a JSON`);
    }
  }

  _subscribe () {
    this.nc.subscribe('register', (err, msg) => {
      if (err) {
        log.error(`Error receiving a registration message: ${err}`);
        return;
      }
      const data = this._parsePayload(msg);
      if (!data) {
        return;
      }
      const ep = data.grpcEndpoint;
      if (typeof ep !== 'string' || ep.length === 0) {
        log.error('Invalid grpc endpoint in registration message');
        return;
      }
      const id = data.id;
      if (typeof id !== 'string' || id.length === 0) {
        log.error('Invalid node name in registration message');
        return;
      }
      log.trace(`"${id}" with "${ep}" requested registration`);
      this.registry.addNode(id, ep);
    });

    this.nc.subscribe('deregister', (err, msg) => {
      if (err) {
        log.error(`Error receiving a deregistration message: ${err}`);
        return;
      }
      const data = this._parsePayload(msg);
      if (!data) {
        return;
      }
      const id = data.id;
      if (typeof id !== 'string' || id.length === 0) {
        log.error('Invalid node name in deregistration message');
        return;
      }
      log.trace(`"${id}" requested deregistration`);
      this.registry.removeNode(id);
    });
  }
}

module.exports = {
  MessageBus
};
