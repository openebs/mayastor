#!/bin/sh

# This is a wrapper script for helm to generate yaml files for deploying
# mayastor. It provides reasonable defaults for helm values based on
# selected profile. Easy to use and minimizing risk of error.
# Keep the script as simple as possible - ad-hoc use cases can be addressed
# by running helm directly.

set -e

SCRIPTDIR="$(realpath "$(dirname "$0")")"

# Internal variables tunable by options
cores=
moac_debug=false
output_dir="$SCRIPTDIR/../deploy"
pools=
profile=
pull_policy=
registry=
tag=

help() {
  cat <<EOF

Usage: $0 [OPTIONS] <PROFILE>

Common options:
  -c <cores>       # of cpu cores for mayastor overriding the profile's default.
  -h/--help        Display help message and exit.
  -o <output_dir>  Directory to store the generated yaml files (default $output_dir)
  -p <node,device> Node name and associated pool device (the option may repeat).
  -r <registry>    Docker image registry of mayastor images (default none).
  -t <tag>         Tag of mayastor images overriding the profile's default.

Profiles:
  develop:   Used by developers of mayastor.
  release:   Recommended for stable releases deployed by users.
  test:      Used by mayastor e2e tests.
EOF
}

# Parse common options
while [ "$#" -gt 0 ]; do
  case "$1" in
    -c)
      shift
      cores=$1
      ;;
    -h|--help)
      help
      exit 0
      ;;
    -o)
      shift
      output_dir=$1
      ;;
    -p)
      shift
      pools="$pools $1"
      ;;
    -r)
      shift
      registry=$1
      ;;
    -t)
      shift
      tag=$1
      ;;
    -*)
      echo "Unknown option: $1"
      help
      exit 1
      ;;
    *)
      profile=$1
      shift
      break
      ;;
  esac
  shift
done

# The space after profile name is reserved for profile specific options which
# we don't have yet.
if [ "$#" -gt 0 ]; then
  help
  exit 1
fi

# In most of the cases the tag will be a specific version that does not change
# so save dockerhub bandwidth and don't always pull the image.
if [ -n "$tag" ]; then
  pull_policy=IfNotPresent
else
  pull_policy=Always
fi

# Set profile defaults
case "$profile" in
  "develop")
    [ -z "$cores" ] && cores=1
    [ -z "$tag" ] && tag=develop
    moac_debug=true
    ;;
  "release")
    [ -z "$cores" ] && cores=1
    [ -z "$tag" ] && tag=latest
    ;;
  "test")
    [ -z "$cores" ] && cores=2
    [ -z "$tag" ] && tag=ci
    moac_debug=true
    ;;
  *)
    echo "Missing or invalid profile name. Type \"$0 --help\""
    exit 1
    ;;
esac

set -u

if ! which helm >/dev/null 2>&1; then
  echo "Install helm (>v3.4.1) to PATH"
  exit 1
fi

if [ ! -d "$output_dir" ]; then
  mkdir -p "$output_dir"
fi

tmpd=$(mktemp -d /tmp/generate-deploy-yamls.sh.XXXXXXXX)
# shellcheck disable=SC2064
trap "rm -fr '$tmpd'" HUP QUIT EXIT TERM INT

# A rule of thumb: # of cpu cores equals to Gigs of hugepage memory
template_params="mayastorImagesTag=$tag"
template_params="$template_params,mayastorImagePullPolicy=$pull_policy"
template_params="$template_params,mayastorCpuCount=$cores"
template_params="$template_params,mayastorHugePagesGiB=$cores"
template_params="$template_params,moacDebug=$moac_debug"
if [ -n "$registry" ]; then
  template_params="$template_params,mayastorImagesRegistry=$registry"
fi
if [ -n "$pools" ]; then
  i=0
  for pool in $pools; do
    node=$(echo "$pool" | sed 's/,.*//')
    device=$(echo "$pool" | sed 's/.*,//')
    template_params="$template_params,mayastorPools[$i].node=$node"
    template_params="$template_params,mayastorPools[$i].device=$device"
    i=$((i + 1))
  done
fi

helm template --set "$template_params" mayastor "$SCRIPTDIR/../chart" --output-dir="$tmpd" --namespace mayastor

mv "$tmpd"/mayastor/templates/*.yaml "$output_dir/"
