#! /usr/bin/env bash

set -euxo pipefail
cd "$(dirname ${BASH_SOURCE[0]})"

pushd setup
  ./bringup-cluster.sh
popd

../../scripts/release.sh --private-registry "172.18.8.101:30291" --skip-publish-to-dockerhub

pushd install
  go test
popd

