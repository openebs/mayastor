#!/usr/bin/env bash

set -e

cd "$(dirname ${BASH_SOURCE[0]})"

timeout=1000

(cd replica_disconnect && go test -timeout "${timeout}s")

# the following test requires a cluster with at least 4 nodes
(cd replica_reassign && go test -timeout "${timeout}s")
