// Interface to the NATS server where mayastor instances send registration
// requests and events.

import assert from 'assert';
import * as nats from 'nats';

import { Registry } from './registry';
import { Logger } from './logger';

const log = Logger('nats');

// If NATS server is not available then keep trying to connect in this interval
const RECONNECT_DELAY = 10000; // in ms

type RegistrationMsg = {
  id: string;
  grpcEndpoint: string;
};

type DeregistrationMsg = {
  id: string;
};

// Message bus object subscribes to messages from NATS server and handles each
// message by dispatching it further to other moac components.
export class MessageBus {
  registry: Registry;
  endpoint: string;
  nc: nats.NatsConnection | null;
  timeout: NodeJS.Timeout | null;
  connected: boolean;
  reconnectDelay: number;

  // Create a new message bus object.
  //
  // @param {object} registry        Object registry used for adding/removing of nodes.
  // @param {object} reconnectDelay  If NATS srv is unavailable, keep trying with this delay.
  constructor (registry: Registry, reconnectDelay?: number) {
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
  start (endpoint: string) {
    assert(!this.nc);
    this.endpoint = endpoint;
    this._connect();
  }

  // Disconnect from the NATS server
  stop () {
    if (this.timeout) {
      clearTimeout(this.timeout);
      this.timeout = null;
    }
    this._disconnect();
  }

  // Return if the bus is connected to the NATS server.
  //
  // @returns {boolean} true if connected otherwise false.
  isConnected (): boolean {
    return this.connected;
  }

  // The method is async but returns immediately.
  // However it's up to caller if she wants to wait for it.
  _connect () {
    log.debug(`Connecting to NATS at "${this.endpoint}" ...`);
    if (this.timeout) clearTimeout(this.timeout);
    assert(!this.nc);
    nats.connect({
      servers: [`nats://${this.endpoint}`]
    })
      .then((nc) => {
        log.info(`Connected to NATS message bus at "${this.endpoint}"`);
        this.nc = nc;
        this.connected = true;
        this._subscribe();
      })
      .catch((err) => {
        log.error(`${err}`);
        this._disconnect();
        log.debug(`Reconnecting after ${this.reconnectDelay}ms`);
        // reconnect but give it some time to recover to prevent spinning in loop
        this.timeout = setTimeout(this._connect.bind(this), this.reconnectDelay);
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

  _parsePayload (msg: nats.Msg) {
    const sc = nats.StringCodec();
    try {
      return JSON.parse(sc.decode(msg.data));
    } catch (e) {
      log.error(`Invalid payload in ${msg.subject} message: not a JSON`);
    }
  }

  _registrationReceived (data: RegistrationMsg) {
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
  }

  _deregistrationReceived (data: DeregistrationMsg) {
    const id = data.id;
    if (typeof id !== 'string' || id.length === 0) {
      log.error('Invalid node name in deregistration message');
      return;
    }
    log.trace(`"${id}" requested deregistration`);
    this.registry.disconnectNode(id);
  }

  _subscribe () {
    assert(this.nc);
    const registrySub = this.nc.subscribe('v0/registry');
    this._registryHandler(registrySub);
  }

  async _registryHandler (sub: nats.Subscription) {
    for await (const m of sub) {
      const payload = this._parsePayload(m);
      if (!payload) {
        return;
      }
      if (payload.id === 'v0/register') {
        this._registrationReceived(payload.data);
      } else if (payload.id === 'v0/deregister') {
        this._deregistrationReceived(payload.data);
      } else {
        const id = payload.id;
        log.error(`Unknown registry message: ${id}`);
      }
    }
  }
}