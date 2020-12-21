#! /bin/sh

set -e

if [ "x$1" = x ]; then
cat <<EOF
USAGE: $0 [-t <target_dir>] <mayastor_docker_images_tag> [<mayastor_images_repo>]

Generate (some) deployment YAMLs from the helm chart and store them to deploy/
in the repo. If -t is specified do not put them to deploy/ but rather to the
directory given.
EOF
	exit 1
fi

SCRIPTDIR="$(realpath "$(dirname "$0")")"

if [ "$1" = "-t" ]; then
	TARGET_DIR="$2"
	shift 2
else
	TARGET_DIR="$SCRIPTDIR/../deploy"
fi
if [ ! -d "$TARGET_DIR" ]; then
	mkdir -p "$TARGET_DIR"
fi

if [ "x$2" = x ]; then
	mayastor_images_repo="NONE"
else
	mayastor_images_repo="$2"
fi

set -u

if ! which helm > /dev/null 2>&1; then
	echo "Install helm to path >v3.4.1"
	echo "https://github.com/helm/helm/releases/tag/v3.4.1"
	exit 1
fi

tmpd=$(mktemp -d /tmp/generate-deploy.sh.XXXXXXXX)
# shellcheck disable=SC2064
trap "rm -fr '$tmpd'" HUP QUIT EXIT TERM INT

if [ "$mayastor_images_repo" = "NONE" ]; then
	helm template --set "mayastorImagesTag=$1" mayastor "$SCRIPTDIR/../chart" --output-dir="$tmpd" --namespace mayastor
else
	helm template --set "mayastorImagesTag=$1,mayastorImagesRepo=$mayastor_images_repo" mayastor "$SCRIPTDIR/../chart" --output-dir="$tmpd" --namespace mayastor
fi

mv "$tmpd"/mayastor/templates/*.yaml "$TARGET_DIR"
