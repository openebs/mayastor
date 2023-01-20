#!/usr/bin/env bash

source ${BASH_SOURCE%/*}/rust-linter-env.sh
$CARGO clippy --all --all-targets -- -D warnings \
    -A clippy::await-holding-lock \
    -A clippy::await-holding-refcell-ref \
    -A clippy::borrow-deref-ref \
    -A clippy::box-default \
    -A clippy::explicit-auto-deref \
    -A clippy::let-and-return \
    -A clippy::let-underscore-future \
    -A clippy::let-unit-value \
    -A clippy::manual-retain \
    -A clippy::map-flatten \
    -A clippy::needless-borrow \
    -A clippy::needless-late-init \
    -A clippy::question-mark \
    -A clippy::result-large-err \
    -A clippy::single-char-pattern \
    -A clippy::type-complexity \
    -A clippy::uninlined-format-args \
    -A clippy::unnecessary-cast \
    -A clippy::unnecessary-lazy-evaluations \
    -A clippy::unnecessary-to-owned \
    -A clippy::unwrap-or-else-default \
    -A clippy::useless-conversion
