#!/usr/bin/env sh

set -eu

# This script is used mainly by ci/cd system to discover new coredumps on the
# system after running tests.

# Globals
since="2000-01-01 00:00:00"

help() {
  cat <<EOF
Usage: $0 [--since DATE]

Options:
  --since DATE   Report only coredumps newer than provided date.
                 (DATE format is YYYY-MM-DD HH:MM:SS - due to space needs quotes!)

Example:
  $0 --since "2021-04-31 12:45:56"
EOF
}

# Parse arguments
while [ "$#" -gt 0 ]; do
  case "$1" in
    --since)
      shift
      since="$1"
      ;;
    -h|--help)
      help
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      help
      exit 1
      ;;
  esac
  shift
done

if ! coredumpctl --version >/dev/null 2>&1; then
  echo "Missing required command coredumpctl in PATH"
  exit 1;
fi
if ! gdb --version >/dev/null 2>&1; then
  echo "Missing required command gdb in PATH"
  exit 1
fi

# Iterate over new coredumps and print a summary and stack for each
echo "Looking for new coredumps ..."
echo
coredump_pids=$(coredumpctl list --quiet --no-legend --since="$since" | awk '{ print $5 }')
coredump_count=0
for pid in $coredump_pids; do
  coredump_count=$((coredump_count + 1))
  # The core file might not exist and we want to continue examining
  # other cores in such case so we have to append "true".
  echo "thread apply all bt" | coredumpctl gdb "$pid" || true
done

if [ "$coredump_count" -gt 0 ]; then
  echo "Found $coredump_count new coredumps!"
  exit 1
fi
echo
echo "No new coredumps"
