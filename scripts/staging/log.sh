#!/usr/bin/env bash

# Write output to error output stream.
log_to_stderr() {
  echo -e "${1}" >&2
}

log_error() {
  log_to_stderr "ERROR: $1"
}

log_warn() {
  log_to_stderr "WARNING: $1"
}

# Exit with error status and print error.
log_fatal() {
  local -r _return="${2:-1}"
  log_error "$1"
  exit "${_return}"
}

log() {
  echo -e "${1}"
}