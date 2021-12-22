#!/usr/bin/env bash

err_exit() {
  echo "Rust linter: $1"
  exit 1
}

make_regex() {
  local res=$1
  local res=$(echo "$res" | sed "s/\\./\\\\./g")
  local res=$(echo "$res" | sed "s/(/\\\\(/g")
  local res=$(echo "$res" | sed "s/)/\\\\)/g")
  local res=$(echo "$res" | sed "s/\\*/.*/g")
  echo "$res"
}

echo "configuring Rust linter environment..."

if [[ $(uname) != "Linux" ]]; then
  err_exit "must be run on Linux with Mayastor prerequisities installed"
fi

# Rust nightly toolchain "2021-11-30" installs cargo version "2021-11-24" and
# rustfmt and clippy "2021-11-29".
# When upgrading Rust toolchain version, check 'cargo --version',
# 'cargo fmt --version', 'cargo clippy --version' and put them here.
RUST_TOOLCHAIN_VER="2021-11-30"
WANTED_CARGO_VER="cargo 1.58.0-nightly (* 2021-11-24)"
WANTED_RUSTFMT_VER="rustfmt 1.4.38-nightly (* 2021-11-29)"
WANTED_CLIPPY_VER="clippy 0.1.58 (* 2021-11-29)"
CARGO="cargo"
CARGO_MODE="system default"

CARGO_VER_RE=$(make_regex "$WANTED_CARGO_VER")
RUSTFMT_VER_RE=$(make_regex "$WANTED_RUSTFMT_VER")
CLIPPY_VER_RE=$(make_regex "$WANTED_CLIPPY_VER")

# If RUST_NIGHTLY_PATH is defined, use it.
if [ -n "$RUST_NIGHTLY_PATH" ]; then
  CARGO="$RUST_NIGHTLY_PATH/bin/cargo"
  CARGO_MODE="explicit nightly path"

  # cargo searches PATH for its subcommands (e.g. cargo fmt), so export
  # PATH with RUST_NIGHTLY_PATH put before all other PATHs.
  PATH=$RUST_NIGHTLY_PATH/bin:$PATH
fi

# Try cargo from PATH.
CARGO_VER=$($CARGO --version 2>/dev/null)

if [[ $? != 0 ]]; then
  err_exit "'cargo' not found"
fi

if ! [[ "$CARGO_VER" =~ $CARGO_VER_RE ]]; then
  # Try +nightly option of cargo installed via rustup.
  # Give up, if it fails.
  CARGO="cargo +nightly-$RUST_TOOLCHAIN_VER"
  CARGO_VER=$($CARGO --version 2>/dev/null)
  CARGO_MODE="rustup nightly cargo"
  if [[ $? != 0 ]] || ! [[ "$CARGO_VER" =~ $CARGO_VER_RE ]]; then
    err_exit "toolchain with $WANTED_CARGO_VER must be installed (e.g. 'rustup toolchain install nightly-$RUST_TOOLCHAIN_VER')"
  fi
fi

# Check fmt version.
RUSTFMT_VER=$($CARGO fmt --version 2>/dev/null)
if [[ $? != 0 ]]; then
  err_exit "'cargo fmt' not found"
fi

if ! [[ "$RUSTFMT_VER" =~ $RUSTFMT_VER_RE ]]; then
  err_exit "toolchain with $WANTED_RUSTFMT_VER must be installed (e.g. 'rustup toolchain install nightly-$RUST_TOOLCHAIN_VER')"
fi

# Check clippy version.
CLIPPY_VER=$($CARGO clippy --version 2>/dev/null)
if [[ $? != 0 ]]; then
  err_exit "'cargo clippy' not found"
fi

if ! [[ "$CLIPPY_VER" =~ $CLIPPY_VER_RE ]]; then
  err_exit "toolchain with $WANTED_CLIPPY_VER must be installed (e.g. 'rustup toolchain install nightly-$RUST_TOOLCHAIN_VER')"
fi

echo "  using cargo          : $CARGO_MODE"
echo "  cargo command        : $CARGO"
echo "  cargo version        : $CARGO_VER"
echo "  cargo fmt version    : $RUSTFMT_VER"
echo "  cargo clippy version : $CLIPPY_VER"
echo "configuration of Rust linter environment done"
