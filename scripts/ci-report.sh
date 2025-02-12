#!/usr/bin/env bash

SCRIPT_DIR="$(dirname "$0")"
export ROOT_DIR="$SCRIPT_DIR/.."
SUDO=$(which sudo)
CI_REPORT_START_DATE=${CI_REPORT_START_DATE:--3h}

set -eu

mkdir -p "$ROOT_DIR/ci-report"
cd "$ROOT_DIR/ci-report"

journalctl --since="$CI_REPORT_START_DATE" -o short-precise > journalctl.txt
journalctl -k -b0 -o short-precise > dmesg.txt
lsblk -tfa > lsblk.txt
$SUDO nvme list -v > nvme.txt
$SUDO nvme list-subsys -v >> nvme.txt
cat /proc/meminfo > meminfo.txt

find . -type f \( -name "*.txt" -o -name "*.xml" \) -print0 | xargs -0 tar -czvf ci-report.tar.gz
