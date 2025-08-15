#!/usr/bin/env bash

set -euo pipefail

DEFAULT_NVME_SYSCONFDIR="/etc/nvme"
DEFAULT_NVME_HOSTID="03f79caf-dc58-475a-a111-bf0b75214a51"
DEFAULT_NVME_HOSTNQN="nqn.2014-08.org.nvmexpress:uuid:$DEFAULT_NVME_HOSTID"

display_help() {
  cat <<EOF
 Usage: $(basename "$0") [options]

 Options:
    -a, --apply         Apply the configuration
    -c, --check         Check if the configuration \$SYSCONFDIR exists and is valid.
    -o, --overwrite     Overwrite the existing configuration
    -I, --hostid        Specify the hostid (default: $DEFAULT_NVME_HOSTID)
    -q, --hostnqn       Specify the hostnqn (default: $DEFAULT_NVME_HOSTNQN)
    -s, --sysconfdir    The path where the nvme \$SYSCONFDIR is placed at (default $DEFAULT_NVME_SYSCONFDIR)
    -h, --help          Display this help message

Examples:
  $(basename "$0")
EOF
}

CLI_ARGS="$@"
while [ "$#" -gt 0 ]; do
  case $1 in
    -a|--apply) APPLY="true" ;;
    -c|--check) CHECK="true" ;;
    -o|--overwrite) OVERWRITE="true" ;;
    -I|--hostid) NVME_HOSTID="$2"; APPLY_CONFIG="true"; shift ;;
    -q|--hostnqn) NVME_HOSTNQN="$2"; APPLY_CONFIG="true"; shift ;;
    -s|--sysconfdir) NVME_SYSCONFDIR="$2"; shift ;;
    -h|--help) display_help; exit 0 ;;
    *) echo "Unknown parameter passed: $1"; display_help; exit 1 ;;
  esac
  shift
done

if [ -z "${NVME_SYSCONFDIR:-}" ]; then
  NVME_SYSCONFDIR="$DEFAULT_NVME_SYSCONFDIR"
fi
if [ -z "${NVME_HOSTID:-}" ]; then
  NVME_HOSTID="$DEFAULT_NVME_HOSTID"
fi
if [ -z "${NVME_HOSTNQN:-}" ]; then
  NVME_HOSTNQN="$DEFAULT_NVME_HOSTNQN"
fi
NVME_SYSCONFDIR_HOSTID_P="$NVME_SYSCONFDIR/hostid"
NVME_SYSCONFDIR_HOSTNQN_P="$NVME_SYSCONFDIR/hostnqn"
NVME_SYSCONFDIR_EXISTS="false"
NVME_SYSCONFDIR_HOSTID=""
NVME_SYSCONFDIR_HOSTNQN=""
NVME_SYSCONFDIR_HOSTID_EXISTS="false"
NVME_SYSCONFDIR_HOSTNQN_EXISTS="false"


if [ -d "$NVME_SYSCONFDIR" ]; then
  NVME_SYSCONFDIR_EXISTS="true"
  if [ -f "$NVME_SYSCONFDIR_HOSTID_P" ]; then
    NVME_SYSCONFDIR_HOSTID_EXISTS="true"
    NVME_SYSCONFDIR_HOSTID="$(cat "$NVME_SYSCONFDIR_HOSTID_P")"
  fi

  if [ -f "$NVME_SYSCONFDIR_HOSTID_P" ]; then
    NVME_SYSCONFDIR_HOSTNQN_EXISTS="true"
    NVME_SYSCONFDIR_HOSTNQN="$(cat "$NVME_SYSCONFDIR_HOSTNQN_P")"
  fi
fi


echo " NVME SYSCONFDIR   : $NVME_SYSCONFDIR"
echo "|           exists : $NVME_SYSCONFDIR_EXISTS"
echo "|= hostid          : $NVME_SYSCONFDIR_HOSTID_P"
echo "|           exists : $NVME_SYSCONFDIR_HOSTID_EXISTS"
echo "|          content : $NVME_SYSCONFDIR_HOSTID"
echo "|= hostnqn         : $NVME_SYSCONFDIR_HOSTNQN_P"
echo "|           exists : $NVME_SYSCONFDIR_HOSTNQN_EXISTS"
echo "|          content : $NVME_SYSCONFDIR_HOSTNQN"
echo ""
echo ""
if [ "${APPLY_CONFIG:-}" = "true" ] && [ "${APPLY:-}" = "true" ]; then
  echo " Requested config  : $NVME_SYSCONFDIR"
  echo "| hostid           : $NVME_HOSTID"
  echo "| hostnqn          : $NVME_HOSTNQN"
  echo ""
  echo ""
fi

if [ "${CHECK:-}" = "true" ]; then
  if [ -z "$NVME_SYSCONFDIR_HOSTID" ] || [ -z "$NVME_SYSCONFDIR_HOSTNQN" ]; then
    CONFIG_VALID="false"
    echo "ERROR: \$SYSCONFDIR at $NVME_SYSCONFDIR is not valid" >&2
    echo ""
    if [ "${APPLY:-}" != "true" ]; then
      exit 1
    fi
  else
    echo "INFO: $NVME_SYSCONFDIR is valid"
    echo ""
    if [ "${APPLY:-}" != "true" ] || [ "${APPLY_CONFIG:-}" != "true" ]; then
      exit 0
    fi
  fi
fi

if [ "${NVME_SYSCONFDIR_HOSTID:-}" = "${NVME_HOSTID:-}" ] && [ "${NVME_SYSCONFDIR_HOSTNQN:-}" = "${NVME_HOSTNQN:-}" ]; then
  echo "INFO: $NVME_SYSCONFDIR matches the requested configuration"
  exit 0
fi

if [ "${CONFIG_VALID:-}" != "false" ]; then
  echo "WARNING: $NVME_SYSCONFDIR does not match the requested configuration" >&2
  echo ""
fi

if [ "${APPLY:-}" != "true" ]; then
  echo "ERROR: Not applying, rerun with --apply if you wish to apply" >&2
  exit 1
fi

echo "INFO: Applying ..."
echo ""

if [ "${NVME_SYSCONFDIR_HOSTID_EXISTS:-}" = "true" ] || [ "${NVME_SYSCONFDIR_HOSTNQN_EXISTS:-}" = "true" ]; then
  if [ "${OVERWRITE:-}" != "true" ]; then
    echo "ERROR: Not overwriting, rerun with --overwrite if you wish to overwrite" >&2
    exit 1
  fi
  echo "INFO: Overwriting ..."
  echo ""
fi

if [ ! -w "$(dirname $NVME_SYSCONFDIR)" ]; then
  echo "ERROR: Please rerun with enough permissions (ex: sudo $0 $CLI_ARGS)" >&2
  exit 1
fi

if [ ! -d "$NVME_SYSCONFDIR" ]; then
  echo "INFO: Creating $NVME_SYSCONFDIR ..."
  mkdir -p "$NVME_SYSCONFDIR"
  echo ""
fi

if [ ! -f "$NVME_SYSCONFDIR_HOSTID_P" ]; then
  touch "$NVME_SYSCONFDIR_HOSTID_P"
fi
if [ ! -f "$NVME_SYSCONFDIR_HOSTNQN_P" ]; then
  touch "$NVME_SYSCONFDIR_HOSTNQN_P"
fi

if [ ! -w $NVME_SYSCONFDIR_HOSTID_P ] || [ ! -w $NVME_SYSCONFDIR_HOSTNQN_P ]; then
  echo "ERROR: Please rerun with enough permissions (ex: sudo $0 $CLI_ARGS" >&2
  exit 1
fi

echo "INFO: Writing $NVME_HOSTID into $NVME_SYSCONFDIR_HOSTID_P ..."
echo "$NVME_HOSTID" > "$NVME_SYSCONFDIR_HOSTID_P"

echo ""

echo "INFO: Writing $NVME_HOSTNQN into $NVME_SYSCONFDIR_HOSTNQN_P ..."
echo "$NVME_HOSTNQN" > "$NVME_SYSCONFDIR_HOSTNQN_P"
echo ""

echo "Done"
