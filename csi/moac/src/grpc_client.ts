// gRPC client related utilities

import assert from 'assert';
import * as path from 'path';
import { Logger } from './logger';

const log = Logger('grpc');

const grpc = require('grpc-uds');
const grpcPromise = require('grpc-promise');
const protoLoader = require('@grpc/proto-loader');

const MAYASTOR_PROTO_PATH: string = path.join(__dirname, '../proto/mayastor.proto');

// Load mayastor proto file
const packageDefinition = protoLoader.loadSync(MAYASTOR_PROTO_PATH, {
  // this is to load google/descriptor.proto
  includeDirs: ['./node_modules/protobufjs'],
  keepCase: false,
  longs: Number,
  enums: String,
  defaults: true,
  oneofs: true
});
export const mayastor = grpc.loadPackageDefinition(packageDefinition).mayastor;
export const grpcCode: Record<string, number> = grpc.status;

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
  handle: any;

  // Create promise-friendly grpc client handle.
  //
  // @param endpoint   Host and port that mayastor server listens on.
  constructor (endpoint: string) {
    const handle = new mayastor.Mayastor(
      endpoint,
      grpc.credentials.createInsecure()
    );
    grpcPromise.promisifyAll(handle);
    this.handle = handle;
  }

  // Call a grpc method with arguments.
  //
  // @param method   Name of the grpc method.
  // @param args     Arguments of the grpc method.
  // @returns Return value of the grpc method.
  async call (method: string, args: any) {
    log.trace(
      `Calling grpc method ${method} with arguments: ${JSON.stringify(args)}`
    );
    const ret = await this.handle[method]().sendMessage(args);
    log.trace(`Grpc method ${method} returned: ${JSON.stringify(ret)}`);
    return ret;
  }

  // Close the grpc handle. The client should not be used after that.
  close () {
    this.handle.close();
  }
}