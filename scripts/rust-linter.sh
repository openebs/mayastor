#!/usr/bin/env bash

source ${BASH_SOURCE%/*}/rust-linter-env.sh
$CARGO clippy --all --all-targets -- -D warnings \
    -A clippy::await-holding-lock \
    -A clippy::await-holding-refcell-ref \
    -A clippy::result-large-err \
    -A clippy::type-complexity \
    -A clippy::option_env_unwrap \
    -A clippy::redundant-guards \
    -A clippy::suspicious-doc-comments \
    -A clippy::useless-format \
    -A deprecated
