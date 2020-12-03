#!/usr/bin/env bash

set -e
timeout=200

# TODO run setup test here
(cd node_disconnect_nvmf_reject && go test --timeout "${timeout}s")
(cd node_disconnect_iscsi_reject && go test --timeout "${timeout}s")

# These tests currently fail
# (cd node_disconnect_nvmf_drop && go test --timeout "${timeout}s")
# (cd node_disconnect_iscsi_drop && go test --timeout "${timeout}s")

