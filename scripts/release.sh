#!/usr/bin/env bash

# Build and upload mayastor docker images to dockerhub repository.
# Use --dry-run to just see what would happen.
# The script assumes that a user is logged on to dockerhub for public images,
# or has insecure registry access setup for CI.

# Allow override from caller
if [[ -z "${SOURCE_REL:-}" ]]; then
    SOURCE_REL=$(dirname "$0")/../utils/dependencies/scripts/release.sh
fi

if [ ! -f "$SOURCE_REL" ] && [ -z "$CI" ]; then
  git submodule update --init --recursive
fi

IMAGES="mayastor.io-engine mayastor.casperf fio-spdk"
CARGO_DEPS=units.cargoDeps
PROJECT="io-engine"

. "$SOURCE_REL"

if [ "${NO_RUN:-}" != "true" ]; then
  common_run "$@"
fi
