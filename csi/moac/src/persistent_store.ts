// Interface to the persistent store (etcd) where mayastor instances register nexus information
// such as the list of nexus children and their health state and whether the nexus was shutdown
// cleanly or not.

import assert from 'assert';
import { Etcd3, IOptions } from 'etcd3';
import { defaults } from 'lodash';
import { Replica } from './replica';
import { Logger } from './logger';

const log = Logger('store');

// Definition of the nexus information that gets saved in the persistent store.
export class NexusInfo {
  // Nexus destroyed successfully.
  cleanShutdown: boolean;
  // Information about children.
  children: ChildInfo[];

  constructor (object: { [k: string]: any }) {
    this.cleanShutdown = 'clean_shutdown' in object ? object['clean_shutdown'] : object['cleanShutdown'];
    this.children = object['children'];
  }
}

// Definition of the child information that gets saved in the persistent store.
export class ChildInfo {
  // UUID of the child.
  uuid: string;
  // Child's state of health.
  healthy: boolean;

  constructor (object: { [k: string]: any }) {
    this.uuid = object['uuid'];
    this.healthy = object['healthy'];
  }
}

export class PersistentStore implements NexusCreateInfo {
  private client: Etcd3;
  private endpoints: string[];
  private externClient?: () => Etcd3;
  // 1 minute timeout by default
  private timeoutMs: number = 60000;
  // In some conditions, the grpc call to etcd may take up to 15 minutes to fail, even when the etcd
  // is already up again. Forcing a cancelation and allowing a quicker retry seems to alliviate this issue.
  private promiseWithTimeout = (prom: Promise<any>, timeoutMs: number, exception: any) => {
    let timer: NodeJS.Timeout;
    return Promise.race([
      prom,
      new Promise((_r, rej) => timer = setTimeout(rej, timeoutMs, exception))
    ]).finally(() => clearTimeout(timer));
  }

  // Get default etcd client options, could be used to configure retries and timeouts...
  // @param   {string[]} endpoints   List of etcd endpoints to connect to
  // @returns {IOptions}             Options for the etcd client
  //
  private getOptions (endpoints: string[]): IOptions {
    return {
      hosts: endpoints,
      ...defaults
    };
  }

  // Returns a new etcd client
  // @param   {Etcd3}   client    New etcd client object
  //
  private newClient (): Etcd3 {
    log.debug("Creating a new etcd client...");

    if (this.externClient !== undefined)
      return this.externClient();
    else
      return new Etcd3(this.getOptions(this.endpoints));
  }

  // Sets up the persistent store (note, at this point it does not wait for any connection to be established)
  // @param   {string[]}     endpoints   List of etcd endpoints to connect to
  // @param   {?number}      timeoutMs   Promise timeout while waiting for a reply from etcd
  // @param   {?()=>Etcd3?}  client      Alternative etcd client, used by the mock tests
  //
  constructor (endpoints: string[], timeoutMs?: number, client?: () => Etcd3) {
    this.endpoints = endpoints.map((e) => e.includes(':') ? e : `${e}:2379`);

    this.externClient = client;
    this.client = this.newClient();

    if (timeoutMs !== undefined) {
      this.timeoutMs = timeoutMs;
    }
  }

  // Validates that the info object is a valid NexusInfo object, as per the proto files
  // @param   {INexusInfo | null} info   Nexus info returned by etcd. A null value indicates it does not exist
  // @returns { NexusInfo | null}        Validated Nexus info object with valid parameters, if it exists
  //
  private validateNexusInfo (info: NexusInfo | null): NexusInfo | null {
    // it doesn't exist, just signal that back
    if (!info)
      return info;

    // verify if the inner fields exist
    assert(info.cleanShutdown != null);
    assert(info.children != null);

    // validation passed
    // (no protobuf now, means we can just return the validated object as is)
    return info;
  }

  // Get nexus information from the persistent store
  // @param   {string}            nexusUuid    The uuid of the nexus
  // @returns {NexusInfo | null}               Validated Nexus info object with valid parameters, if it exists
  //                                            or throws an error in case of failure of time out
  //
  private async get_nexus_info (nexusUuid: string): Promise<NexusInfo | null> {
    // get the nexus info as a JSON object
    const promise = this.client.get(nexusUuid).json();
    const timeoutMsg = `Timed out after ${this.timeoutMs}ms while getting the persistent nexus "${nexusUuid}" information from etcd`;
    const timeoutError = Symbol(timeoutMsg);
    try {
      log.debug(`Getting the persistent nexus "${nexusUuid}" information from etcd`);
      const nexusRaw = await this.promiseWithTimeout(promise, this.timeoutMs, timeoutError);
      return this.validateNexusInfo(nexusRaw ? new NexusInfo(nexusRaw) : null);
    } catch (error: any) {
      if (error === timeoutError) {
        this.client = this.newClient();
        throw timeoutMsg;
      }
      throw error;
    }
  }

  // Delete the nexus from the persistent store
  // @param   {string}   nexusUuid    The uuid of the nexus
  // @returns {Promise}               Returns on success or throws an error if it failed|timed out
  //
  private async deleteNexusInfo (nexusUuid: string): Promise<boolean> {
    const timeoutMsg = `Timed out after ${this.timeoutMs}ms while deleting the persistent nexus "${nexusUuid}" information from etcd`;
    const timeoutError = Symbol(timeoutMsg);
    const promise = this.client.delete().key(nexusUuid).exec();
    try {
      log.debug(`Deleting the persistent nexus "${nexusUuid}" information from etcd`);
      const _deleted = await this.promiseWithTimeout(promise, this.timeoutMs, timeoutError);
    } catch (error: any) {
      if (error === timeoutError) {
        this.client = this.newClient();
        throw timeoutMsg;
      }
      throw error;
    }
    return true;
  }

  async filterReplicas (nexusUuid: string, replicas: Replica[]): Promise<Replica[]> {
    const nexus = await this.get_nexus_info(nexusUuid);

    // we have a client AND a nexus does exist for the given uuid
    if (nexus !== null) {
      let filteredReplicas = replicas.filter((r) => {

        let childInfo = nexus.children.find((c) => {
          return c.uuid === r.realUuid;
        });

        // Add only healthy children
        return childInfo?.healthy === true;
      });

      // If the shutdown was not clean then only add 1 healthy child to the create call.
      // This is because children might have inconsistent data.
      if (!nexus.cleanShutdown && filteredReplicas.length > 1) {
        // prefer to keep a local replica, if it exists
        const localReplica = filteredReplicas.findIndex((r) => r.share === 'REPLICA_NONE');
        const singleReplicaIndex = localReplica != -1 ? localReplica : 0;
        filteredReplicas = filteredReplicas.slice(singleReplicaIndex, singleReplicaIndex+1);
      }

      return filteredReplicas;
    } else {
      // If the nexus has never been created then it does not exist in etcd and so we can create
      // it with all the available children as there is no preexisting data.
      return replicas;
    }
  }

  async destroyNexus (nexusUuid: string): Promise<boolean> {
    return this.deleteNexusInfo(nexusUuid);
  }
}

// Exposes persistent Nexus information in a very simplistic manner
export interface NexusCreateInfo {
  // Filter out replicas that cannot be used in the nexus create call, returning only healthy replicas.
  // The remaining replicas may be added later, subject to a rebuild.
  // Throws under error conditions:
  // 1. when it cannot connect to the backing store within a construction timeout.
  // 2. when the client library gives up trying to connect/waiting for the data.
  // 3. when the data retrived from the backing store is invalid.
  // @param   {string}               nexusUuid    The uuid of the nexus
  // @param   {Replica[]}            replicas     Array of replicas to filter on
  // @returns {Promise<Replica[]>}                Returns filtered replicas or throws an error if it failed|timed out
  //
  filterReplicas (nexusUuid: string, replicas: Replica[]): Promise<Replica[]>;
  // Destroy the nexus information from the backing store when it is no long required.
  // @param   {string}    nexusUuid    The uuid of the nexus
  // @returns {Promise}                Returns on success or throws an error if it failed|timed out
  //
  destroyNexus (nexusUuid: string): Promise<boolean>;
}
