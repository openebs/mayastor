#!/usr/bin/env sh

io-engine-client nexus create a01dd9dd-2458-4941-b3c4-d44e14a61393 640Ki 'malloc:///d?size_mb=100'
io-engine-client nexus publish a01dd9dd-2458-4941-b3c4-d44e14a61393
io-engine-client bdev create 'malloc:///d2?size_mb=100'
io-engine-client bdev share d2
io-engine-client nexus add a01dd9dd-2458-4941-b3c4-d44e14a61393 'nvmf://127.0.0.1:8420/nqn.2019-05.io.openebs:d2'
