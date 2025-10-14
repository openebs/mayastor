#!/usr/bin/env bash

set -euo pipefail

submodule_set_branch_all() {
  branch=${1:-}
  if [ -n "$branch" ]; then
    branch="--branch $branch"
  else
    branch="--default"
  fi
  for mod in `git config --file .gitmodules --get-regexp path | awk '{ print $2 }'`; do
    git submodule set-branch $branch $mod
  done
}

submodule_update() {
  local modules="$1"

  if [ -z "$modules" ]; then
    modules=$(git config --file .gitmodules --get-regexp path | awk '{ print $2 }')
  fi

  for mod in $modules; do
    echo "Updating submodule $mod ..." >&2
    git submodule update --remote "$mod"
    pushd "$mod" >/dev/null
    git submodule update --init --recursive .
    popd >/dev/null
  done
}

BRANCH=`git rev-parse --abbrev-ref HEAD`
SET_BRANCH=
CLEAR_BRANCH=
UPDATE=
UPDATE_MODS=
while [ "$#" -gt 0 ]; do
  case $1 in
    -b|--branch)
      shift
      BRANCH=$1
      shift
      ;;
    -c|--clear)
      CLEAR_BRANCH=="y"
      shift
      ;;
    -u|--update)
      UPDATE="y"
      shift
      ;;
    -m|--update-modules)
      shift
      if [ "$1" != " " ]; then
        UPDATE_MODS="${1//,/ }"
      fi
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

if [ "$BRANCH" == "develop" ] || [ "${BRANCH#release/}" != "${BRANCH}" ]; then
  SET_BRANCH="${BRANCH}"
fi

if [ -n "$UPDATE" ]; then
  submodule_update "$UPDATE_MODS"
elif [ -n "$CLEAR_BRANCH" ]; then
  submodule_set_branch_all ""
elif [ -n "$SET_BRANCH" ]; then
  submodule_set_branch_all "$SET_BRANCH"
else
  echo "Not modifying branch"
fi
