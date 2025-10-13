#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(dirname "$(realpath "${BASH_SOURCE[0]:-"$0"}")")"
ROOT_DIR="${SCRIPT_DIR}/../.."
: "${PARENT_ROOT_DIR:=$ROOT_DIR}"

source "$PARENT_ROOT_DIR/scripts/staging/log.sh"

# --- Run release.sh in subshell so it resolves SOURCE_REL correctly ---
(
  export NO_RUN=true
  export CI=1  # avoid unbound variable errors and skip submodule update in CI
  echo "📦 Running release.sh from: $PARENT_ROOT_DIR/scripts/release.sh"
  bash "$PARENT_ROOT_DIR/scripts/release.sh"
)


DOCKERHUB_ORG="${DOCKERHUB_ORG:-openebs}"
IMAGE_REGISTRY="${IMAGE_REGISTRY:-docker.io}"

TRIGGER=""
TAG=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --trigger|--type) TRIGGER="$2"; shift 2 ;;
        --tag) TAG="$2"; shift 2 ;;
        -h|--help)
            cat <<EOF
Usage: $0 --trigger <trigger> [--tag <tag>]
Options:
  --trigger <type>        release, staging, develop, prerelease
  --type <type>           Alias for --trigger
  --tag <tag>             Release tag (e.g., v2.9.0)
EOF
            exit 0 ;;
        *) log_fatal "Unknown option $1" ;;
    esac
done

echo "Validating trigger: $TRIGGER"

case "$TRIGGER" in
    release|staging|develop|prerelease)
        echo "✅ Valid trigger: $TRIGGER"
        ;;
    *)
        log_fatal "❌ Error: Invalid trigger '$TRIGGER'."
        ;;
esac

echo "Validating tag: $TAG"

case "$TRIGGER" in
    release|staging)
        [[ "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-rc\.[0-9]+)?$ ]] \
            || log_fatal "❌ Tag must be in format vX.Y.Z or vX.Y.Z-rc.N"
        ;;
    develop)
        [[ "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+-develop$ ]] \
            || log_fatal "❌ Tag must be in format vX.Y.Z-develop"
        ;;
    prerelease)
        [[ "$TAG" == "v0.0.0" ]] \
            || log_fatal "❌ For prerelease builds, tag must be exactly v0.0.0"
        ;;
esac

echo "✅ Tag validation passed"

# --- Image Validation Section ---
VERSION="${TAG#v}"

dockerhub_tag_exists() {
    local repository="$1" tag="$2"
    curl --silent -f -lSL "https://hub.docker.com/v2/repositories/${repository#docker.io/}/tags/${tag}" >/dev/null 2>&1
}

check_images() {
    if [[ -n "${DEFAULT_IMAGES:-}" ]]; then
        for name in $DEFAULT_IMAGES; do
            image=$($NIX_EVAL -f "$PARENT_ROOT_DIR" "images.$BUILD_TYPE.$name.imageName" --raw --quiet --argstr product_prefix "$PRODUCT_PREFIX")
            image_name="${image##*/}"
            if dockerhub_tag_exists "${DOCKERHUB_ORG}/${image_name}" "${TAG}"; then
                log_fatal "❌ Image ${DOCKERHUB_ORG}/${image_name}:${TAG} already exists"
            else
                echo "✅ Image ${DOCKERHUB_ORG}/${image_name}:${TAG} does not exist"
            fi
        done
    else
        echo "⚠️  No DEFAULT_IMAGES defined — skipping image existence checks."
    fi
}

case "$TRIGGER" in
    staging|release)
        check_images
        ;;
    develop|prerelease)
        echo "Skipping image checks for $TRIGGER"
        ;;
esac

echo "✅ All validations completed successfully"
