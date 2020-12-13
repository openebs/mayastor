#!/usr/bin/env bash

set -e
timeout=200

(cd setup && go test -timeout "${timeout}s")

(cd nvmf_reject && go test -timeout "${timeout}s")
(cd iscsi_reject && go test -timeout "${timeout}s")

(cd nvmf_reject_idle && go test -timeout "${timeout}s")
(cd iscsi_reject_idle && go test -timeout "${timeout}s")

# These tests currently fail
# (cd nvmf_drop && go test -timeout "${timeout}s")
# (cd iscsi_drop && go test -timeout "${timeout}s")

(cd teardown && go test -timeout "${timeout}s")

