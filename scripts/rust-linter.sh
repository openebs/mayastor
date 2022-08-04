#!/usr/bin/env bash

source ${BASH_SOURCE%/*}/rust-linter-env.sh
$CARGO clippy --all --all-targets -- -D warnings
