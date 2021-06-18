#!/usr/bin/env bash

# Build and upload moac docker image to a registry.
# Use --dry-run to just see what would happen.
# The script assumes that a user is logged on to registry (if needed).

set -euo pipefail

# Derives tag name from the git repo. That is git tag value or short commit
# hash if there is no git tag on HEAD.
get_tag() {
  vers=`git tag --points-at HEAD`
  if [ -z "$vers" ]; then
    vers=`git rev-parse --short=12 HEAD`
  fi
  echo -n $vers
}

help() {
  cat <<EOF
Usage: $(basename $0) [OPTIONS]

Options:
  -d, --dry-run              Output actions that would be taken, but don't run them.
  -h, --help                 Display this text.
  --registry <host[:port]>   Push the built images to the provided registry.
  --alias-tag                Explicit alias for short commit hash tag.

Examples:
  $(basename $0) --registry 127.0.0.1:5000
EOF
}

DOCKER="docker"
SCRIPTDIR="$(realpath "$(dirname "$0")")"
TAG=`get_tag`
BRANCH=`git rev-parse --abbrev-ref HEAD`
REGISTRY=
ALIAS=

# Check if all needed tools are installed
curl --version >/dev/null
if [ $? -ne 0 ]; then
  echo "Missing curl - install it and put it to your PATH"
  exit 1
fi
$DOCKER --version >/dev/null
if [ $? -ne 0 ]; then
  echo "Missing docker - install it and put it to your PATH"
  exit 1
fi

# Parse arguments
while [ "$#" -gt 0 ]; do
  case $1 in
    -d|--dry-run)
      DOCKER="echo $DOCKER"
      shift
      ;;
    -h|--help)
      help
      exit 0
      shift
      ;;
    --registry)
      shift
      REGISTRY=$1
      shift
      ;;
    --alias-tag)
      shift
      ALIAS=$1
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

cd $SCRIPTDIR/..

image="mayadata/moac"
if [ -n "$REGISTRY" ]; then
  image="${REGISTRY}/${image}"
fi

# Build it
echo "Building $image:$TAG ..."
$DOCKER build -t "$image:$TAG" "$SCRIPTDIR/.."

# Upload it
echo "Uploading $image:$TAG to registry ..."
$DOCKER push $image:$TAG

# Create alias
alias_tag=
if [ -n "$ALIAS" ]; then
  alias_tag=$ALIAS
elif [ "$BRANCH" == "develop" ]; then
  alias_tag=develop
elif [ "$BRANCH" == "master" ]; then
  alias_tag=latest
fi
if [ -n "$alias_tag" ]; then
  $DOCKER tag $image:$TAG $image:$alias_tag
  $DOCKER push $image:$alias_tag
fi