#!/usr/bin/env bash

FMT_OPTS=${FMT_OPTS:-""}

source ${BASH_SOURCE%/*}/rust-linter-env.sh
$CARGO fmt --all -- $FMT_OPTS
