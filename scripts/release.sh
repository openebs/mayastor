#!/usr/bin/env bash

# Build and upload mayastor docker images to dockerhub repository.
# Use --dry-run to just see what would happen.
# The script assumes that a user is logged on to dockerhub.

set -euo pipefail

docker_tag_exists() {
  curl --silent -f -lSL https://index.docker.io/v1/repositories/$1/tags/$2 1>/dev/null 2>&1
}

get_tag() {
  vers=`git tag --points-at HEAD`
  if [ -z "$vers" ]; then
    vers=`git rev-parse --short HEAD`
  fi
  echo -n $vers
}

DOCKER="docker"
NIX_BUILD="nix-build"
RM="rm"
SCRIPTDIR=$(dirname "$0")
IMAGES="mayastor mayastor-csi moac"
TAG=`get_tag`
BRANCH=`git rev-parse --abbrev-ref HEAD`
UPLOAD=

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
if [ "$#" -gt 0 ]; then
  if [ "$1" == "--dry-run" ]; then
    DOCKER="echo $DOCKER"
    NIX_BUILD="echo $NIX_BUILD"
    RM="echo $RM"
  else
    echo "Usage: release.sh [--dry-run]"
  fi
fi

cd $SCRIPTDIR/..

# Build all images first
for name in $IMAGES; do
  archive=${name}-image
  image=mayadata/$name
  if docker_tag_exists $image $TAG; then
    echo "Skipping $image:$TAG that already exists"
  else
    echo "Building $image:$TAG ..."
    $NIX_BUILD --out-link $archive --no-build-output -A images.$archive
    $DOCKER load -i $archive
    $RM $archive
    UPLOAD="$UPLOAD $image"
  fi
done

# Nothing to upload?
[ -z "$UPLOAD" ] && exit 0

# Upload them
for img in $UPLOAD; do
  echo "Uploading $img:$TAG to registry ..."
  $DOCKER push $img:$TAG
done

# Create aliases
if [ "$BRANCH" == "develop" ]; then
  for img in $UPLOAD; do
    $DOCKER tag $img:$TAG $img:develop
    $DOCKER push $img:develop
  done
elif [ "$BRANCH" == "master" ]; then
  for img in $UPLOAD; do
    $DOCKER tag $img:$TAG $img:latest
    $DOCKER push $img:latest
  done
fi
