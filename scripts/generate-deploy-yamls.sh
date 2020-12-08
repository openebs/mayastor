#! /bin/sh

set -e

if [ "x$1" = x ]; then
cat <<EOF
USAGE: $0 <mayastor_docker_images_tag> [<mayastor_images_repo>]

Generate (some) deployment YAMLs from the helm chart and store them to deploy/
in the repo.
EOF
	exit 1
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

SCRIPTDIR="$(realpath "$(dirname "$0")")"

tmpd=$(mktemp -d /tmp/generate-deploy.sh.XXXXXXXX)
# shellcheck disable=SC2064
trap "rm -fr '$tmpd'" HUP QUIT EXIT TERM INT

if [ "$mayastor_images_repo" = "NONE" ]; then
	helm template --set "mayastorImagesTag=$1" mayastor "$SCRIPTDIR/../chart" --output-dir="$tmpd" --namespace mayastor
else
	helm template --set "mayastorImagesTag=$1,mayastorImagesRepo=$mayastor_images_repo" mayastor "$SCRIPTDIR/../chart" --output-dir="$tmpd" --namespace mayastor
fi

mv "$tmpd"/mayastor/templates/*.yaml "$SCRIPTDIR/../deploy/"
