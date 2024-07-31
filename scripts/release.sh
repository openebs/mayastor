#!/usr/bin/env bash

# Build and upload mayastor docker images to dockerhub repository.
# Use --dry-run to just see what would happen.
# The script assumes that a user is logged on to dockerhub for public images,
# or has insecure registry access setup for CI.

SOURCE_REL=$(dirname "$0")/../utils/dependencies/scripts/release.sh

if [ ! -f "$SOURCE_REL" ] && [ -z "$CI" ]; then
  git submodule update --init --recursive
fi

IMAGES="mayastor.io-engine mayastor.casperf fio-spdk"
CARGO_DEPS=units.cargoDeps
PROJECT="io-engine"
. "$SOURCE_REL"

common_run $@
