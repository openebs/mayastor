#!/usr/bin/env bash

source ${BASH_SOURCE%/*}/rust-linter-env.sh
$CARGO fmt --all -- --check
