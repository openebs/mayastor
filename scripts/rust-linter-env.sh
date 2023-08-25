#!/usr/bin/env bash

MSG_PREFIX="rust linter env"
VERBOSE=0

err_exit() {
  echo "$MSG_PREFIX error: $1"
  exit 1
}

info() {
  if [[ $VERBOSE == "1" ]]; then
    echo "$MSG_PREFIX info: $1"
  fi
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

# Parse arguments
while [ "$#" -gt 0 ]; do
  case $1 in
  -v|--verbose)
    VERBOSE=1
    shift
  ;;
  *)
    err_exit "unknown option: $1"
  ;;
  esac
done

if [[ $(uname) != "Linux" ]]; then
  err_exit "must be run on Linux with Mayastor prerequisities installed"
fi

# Rust nightly toolchain "2021-11-30" installs cargo version "2021-11-24" and
# rustfmt and clippy "2021-11-29".
# When upgrading Rust toolchain version, check 'cargo --version',
# 'cargo fmt --version', 'cargo clippy --version' and put them here.
RUST_TOOLCHAIN_VER="2023-08-25"
WANTED_CARGO_VER="cargo 1.74.0-nightly (* 2023-08-22)"
WANTED_RUSTFMT_VER="rustfmt 1.6.0-nightly (* 2023-08-24)"
WANTED_CLIPPY_VER="clippy 0.1.73 (* 2023-08-24)"
CARGO="cargo"
CARGO_MODE="system"

CARGO_VER_RE=$(make_regex "$WANTED_CARGO_VER")
RUSTFMT_VER_RE=$(make_regex "$WANTED_RUSTFMT_VER")
CLIPPY_VER_RE=$(make_regex "$WANTED_CLIPPY_VER")

# If RUST_NIGHTLY_PATH is defined, use it.
if [ -n "$RUST_NIGHTLY_PATH" ]; then
  info "RUST_NIGHTLY_PATH is set: $RUST_NIGHTLY_PATH"
  CARGO="$RUST_NIGHTLY_PATH/bin/cargo"
  CARGO_MODE="explicit nightly"

  # cargo searches PATH for its subcommands (e.g. cargo fmt), so export
  # PATH with RUST_NIGHTLY_PATH put before all other PATHs.
  PATH=$RUST_NIGHTLY_PATH/bin:$PATH
fi

# Try cargo from PATH.
info "using $CARGO_MODE cargo command: '$CARGO'"
CARGO_VER=$($CARGO --version 2>/dev/null)

if [[ $? != 0 ]]; then
  err_exit "'cargo' not found"
fi

info "found 'cargo' version: $CARGO_VER"

if ! [[ "$CARGO_VER" =~ $CARGO_VER_RE ]]; then
  info "required $WANTED_CARGO_VER, trying rustup +nightly..."

  # Try +nightly option of cargo installed via rustup.
  # Give up, if it fails.
  CARGO="cargo +nightly-$RUST_TOOLCHAIN_VER"
  CARGO_MODE="rustup nightly"
  CARGO_VER=$($CARGO --version 2>/dev/null)
  if [[ $? != 0 ]]; then
    err_exit "'$CARGO' not found"
  fi

  info "using $CARGO_MODE: $CARGO"

  if ! [[ "$CARGO_VER" =~ $CARGO_VER_RE ]]; then
    err_exit "required $WANTED_CARGO_VER"
  fi
fi

#
# Check fmt version.
#
RUSTFMT_VER=$($CARGO fmt --version 2>/dev/null)
if [[ $? != 0 ]]; then
  err_exit "'cargo fmt' not found"
fi

info "found 'cargo fmt' version: $RUSTFMT_VER"

if ! [[ "$RUSTFMT_VER" =~ $RUSTFMT_VER_RE ]]; then
  err_exit "required $WANTED_RUSTFMT_VER"
fi

#
# Check clippy version.
#
CLIPPY_VER=$($CARGO clippy --version 2>/dev/null)

if [[ $? != 0 ]]; then
  err_exit "'cargo clippy' not found"
fi

info "found 'cargo clippy' version: $CLIPPY_VER"

if ! [[ "$CLIPPY_VER" =~ $CLIPPY_VER_RE ]]; then
  err_exit "required $WANTED_CLIPPY_VER"
fi

echo "  using cargo          : $CARGO_MODE"
echo "  cargo command        : $CARGO"
echo "  cargo version        : $CARGO_VER"
echo "  cargo fmt version    : $RUSTFMT_VER"
echo "  cargo clippy version : $CLIPPY_VER"
echo "configuration of Rust linter environment done"
