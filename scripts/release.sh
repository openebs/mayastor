#!/usr/bin/env bash

# Build and upload mayastor docker images to dockerhub repository.
# Use --dry-run to just see what would happen.
# The script assumes that a user is logged on to dockerhub for public images,
# or has insecure registry access setup for CI.

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

help() {
  cat <<EOF
Usage: $(basename $0) [OPTIONS]

Options:
  -d, --dry-run                 Output actions that would be taken, but don't run them.
  -h, --help                    Display this text.
  --private-registry <address>  Push the built images to the provided registry.
  --skip-build                  Don't perform nix-build.
  --skip-publish-to-dockerhub   Don't publish to Dockerhub.

Examples:
  $(basename $0) --private-registry 127.0.0.1:5000
EOF
}

DOCKER="docker"
NIX_BUILD="nix-build"
RM="rm"
SCRIPTDIR=$(dirname "$0")
IMAGES="mayastor mayastor-csi moac"
TAG=`get_tag`
BRANCH=`git rev-parse --abbrev-ref HEAD`
UPLOAD=
SKIP_PUSH_TO_DOCKERHUB=
PRIVATE_REGISTRY=
SKIP_BUILD=

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
      NIX_BUILD="echo $NIX_BUILD"
      RM="echo $RM"
      shift
      ;;
    -h|--help)
      help
      exit 0
      shift
      ;;
    --private-registry)
      shift
      PRIVATE_REGISTRY=$1
      shift
      ;;
    --skip-build)
      SKIP_BUILD="yes"
      shift
      ;;
    --skip-publish-to-dockerhub)
      SKIP_PUSH_TO_DOCKERHUB="yes"
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

cd $SCRIPTDIR/..

for name in $IMAGES; do
  image="mayadata/${name}"
  if [ -z $SKIP_BUILD ]; then
    archive=${name}-image
    if docker_tag_exists $image $TAG; then
      echo "Skipping $image:$TAG that already exists"
    else
      echo "Building $image:$TAG ..."
      $NIX_BUILD --out-link $archive -A images.$archive
      $DOCKER load -i $archive
      $RM $archive
      UPLOAD="$UPLOAD $image"
    fi
  else
    # If we're skipping the build, then we just want to upload
    # the images we already have locally.
    # We should do this for all images.
    UPLOAD="$UPLOAD $image"
  fi
done

# Nothing to upload?
[ -z "$UPLOAD" ] && exit 0

if [ -z $SKIP_PUSH_TO_DOCKERHUB ]; then
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
fi

# If a private registry was specified (ie for ci)
# then we push to it here.
if [ ! -z $PRIVATE_REGISTRY ]; then
  for img in $UPLOAD; do
    $DOCKER tag $img:$TAG ${PRIVATE_REGISTRY}/$img:ci
    $DOCKER push ${PRIVATE_REGISTRY}/$img:ci
  done
fi
