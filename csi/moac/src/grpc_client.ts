// gRPC client related utilities

import assert from 'assert';
import * as path from 'path';
import * as grpc from '@grpc/grpc-js';
import { loadSync } from '@grpc/proto-loader';

import { Logger } from './logger';
import { ServiceClient, ServiceClientConstructor } from '@grpc/grpc-js/build/src/make-client';

const log = Logger('grpc');

const MAYASTOR_PROTO_PATH: string = path.join(__dirname, '../proto/mayastor.proto');
const DEFAULT_TIMEOUT_MS: number = 15000;
const SOFT_TIMEOUT_SLACK_MS: number = 1000;

// Result of loadPackageDefinition() when run on mayastor proto file.
class MayastorDef {
  // Constructor for mayastor grpc service client.
  clientConstructor: ServiceClientConstructor;
  // All enums that occur in mayastor proto file indexed by name
  enums: Record<string, number>;

  constructor() {
    // Load mayastor proto file
    const proto = loadSync(MAYASTOR_PROTO_PATH, {
      // this is to load google/descriptor.proto
      includeDirs: ['./node_modules/protobufjs'],
      keepCase: false,
      longs: Number,
      enums: String,
      defaults: true,
      oneofs: true
    });

    const pkgDef = grpc.loadPackageDefinition(proto).mayastor as grpc.GrpcObject;
    assert(pkgDef && pkgDef.Mayastor !== undefined);
    this.clientConstructor = pkgDef.Mayastor as ServiceClientConstructor;
    this.enums = {};
    Object.values(pkgDef).forEach((ent: any) => {
      if (ent.format && ent.format.indexOf('EnumDescriptorProto') >= 0) {
        ent.type.value.forEach((variant: any) => {
          this.enums[variant.name] = variant.number;
        });
      }
    });
  }
}

export const mayastor = new MayastorDef();

// This whole dance is done to satisfy typescript's type checking
// (not all values in grpc.status are numbers)
export const grpcCode: Record<string, number> = (() => {
  let codes: Record<string, number> = {};
  for (let prop in grpc.status) {
    let val = grpc.status[prop];
    if (typeof val === 'number') {
      codes[prop] = val;
    }
  }
  return codes;
})();

// Grpc error object.
//
// List of grpc status codes:
//   OK: 0,
//   CANCELLED: 1,
//   UNKNOWN: 2,
//   INVALID_ARGUMENT: 3,
//   DEADLINE_EXCEEDED: 4,
//   NOT_FOUND: 5,
//   ALREADY_EXISTS: 6,
//   PERMISSION_DENIED: 7,
//   RESOURCE_EXHAUSTED: 8,
//   FAILED_PRECONDITION: 9,
//   ABORTED: 10,
//   OUT_OF_RANGE: 11,
//   UNIMPLEMENTED: 12,
//   INTERNAL: 13,
//   UNAVAILABLE: 14,
//   DATA_LOSS: 15,
//   UNAUTHENTICATED: 16
//
export class GrpcError extends Error {
  code: number;

  constructor (code: number, msg: string) {
    assert(Object.values(grpcCode).indexOf(code) >= 0);
    super(msg);
    this.code = code;
  }
}

// Implementation of gRPC client encapsulating common code for calling a grpc
// method on a storage node (the node running mayastor).
export class GrpcClient {
  private handle: ServiceClient;
  private timeout: number; // timeout in milliseconds

  // Create promise-friendly grpc client handle.
  //
  // @param endpoint   Host and port that mayastor server listens on.
  // @param [timeout]  Default timeout for grpc methods in millis.
  constructor (endpoint: string, timeout?: number) {
    this.handle = new mayastor.clientConstructor(
      endpoint,
      grpc.credentials.createInsecure()
    );
    this.timeout = (timeout === undefined) ? DEFAULT_TIMEOUT_MS : timeout;
  }

  private promiseWithTimeout = (prom: Promise<any>, timeoutMs: number, exception: any) => {
    let timer: NodeJS.Timeout;
    return Promise.race([
      prom,
      new Promise((_r, rej) => timer = setTimeout(rej, timeoutMs, exception))
    ]).finally(() => clearTimeout(timer));
  }

  // Call a grpc method with arguments.
  //
  // @param method     Name of the grpc method.
  // @param args       Arguments of the grpc method.
  // @param [timeout]  Timeout in ms if the default should not be used.
  // @returns Return value of the grpc method.
  call (method: string, args: any, timeout?: number): Promise<any> {
    log.trace(
      `Calling grpc method ${method} with arguments: ${JSON.stringify(args)}`
    );
    if (timeout === undefined) {
      timeout = this.timeout;
    }
    let promise = new Promise((resolve, reject) => {
      const metadata = new grpc.Metadata();
      metadata.set('grpc-timeout', `${timeout}m`);
      this.handle[method](args, metadata, (err: Error, val: any) => {
        if (err) {
          log.trace(`Grpc method ${method} failed: ${err}`);
          reject(err);
        } else {
          log.trace(`Grpc method ${method} returned: ${JSON.stringify(val)}`);
          resolve(val);
        }
      });
    });

    // In some conditions, the grpc-timeout we've set above is not respected and the call simply gets stuck.
    // When the grpc-timeout is not triggered then trigger our own soft timeout which is the original
    // timeout plus some added slack.
    const softTimeout = timeout + SOFT_TIMEOUT_SLACK_MS;
    const error = new GrpcError(grpcCode.DEADLINE_EXCEEDED, `Soft timeout after ${softTimeout}ms`);
    return this.promiseWithTimeout(promise, softTimeout, error);
  }

  // Close the grpc handle. The client should not be used after that.
  close () {
    this.handle.close();
  }
}