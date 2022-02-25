#!/usr/bin/env bash

set -euxo pipefail

if [ "${SRCDIR:-unset}" = unset ]
then
  echo "SRCDIR must be set to the root of your working tree" 2>&1
  exit 1
fi

cd "$SRCDIR"

python -m grpc_tools.protoc --proto_path=rpc/mayastor-api/protobuf --grpc_python_out=test/python --python_out=test/python mayastor.proto
python -m grpc_tools.protoc --proto_path=rpc/mayastor-api/protobuf/v1 --grpc_python_out=test/python --python_out=test/python \
  bdev.proto common.proto nexus.proto pool.proto replica.proto host.proto

virtualenv --no-setuptools test/python/venv
(source ./test/python/venv/bin/activate && pip install -r test/python/requirements.txt)
