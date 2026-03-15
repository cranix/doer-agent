#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
agent_dir="$(cd "${script_dir}/.." && pwd)"

cd "${agent_dir}"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/build.sh [options]

Build the agent image with docker buildx build and load it locally.

Options:
  --image <name>               Target image repository (default: cranix/doer-agent)
  --tag <tag>                  Image tag (default: current git short SHA, fallback: latest)
  --platform <platform>        Docker target platform (default: linux/amd64)
  --context <dir>              Docker build context (default: .)
  --dockerfile <path>          Dockerfile path (default: ./Dockerfile)
  --also-latest                Also tag the image as :latest
  -h, --help                   Show this help

Examples:
  ./scripts/build.sh
  ./scripts/build.sh --tag v1.2.3 --also-latest
  ./scripts/build.sh --image docker.io/example/doer-agent --platform linux/amd64
EOF
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command not found: $1" >&2
    exit 1
  fi
}

image_name="cranix/doer-agent"
tag="$(git rev-parse --short HEAD 2>/dev/null || true)"
platform="linux/amd64"
context_dir="."
dockerfile="./Dockerfile"
also_latest="false"

if [[ -z "$tag" ]]; then
  tag="latest"
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --image)
      image_name="${2:-}"
      shift 2
      ;;
    --tag)
      tag="${2:-}"
      shift 2
      ;;
    --platform)
      platform="${2:-}"
      shift 2
      ;;
    --context)
      context_dir="${2:-}"
      shift 2
      ;;
    --dockerfile)
      dockerfile="${2:-}"
      shift 2
      ;;
    --also-latest)
      also_latest="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

require_command docker

if ! docker buildx version >/dev/null 2>&1; then
  echo "docker buildx is required." >&2
  exit 1
fi

if [[ -z "$image_name" ]]; then
  echo "--image must not be empty." >&2
  usage >&2
  exit 1
fi

if [[ -z "$tag" ]]; then
  echo "--tag must not be empty." >&2
  exit 1
fi

if [[ ! -d "$context_dir" ]]; then
  echo "Context directory not found: $context_dir" >&2
  exit 1
fi

if [[ ! -f "$dockerfile" ]]; then
  echo "Dockerfile not found: $dockerfile" >&2
  exit 1
fi

image_refs=()
image_refs+=("${image_name}:${tag}")

if [[ "$also_latest" == "true" && "$tag" != "latest" ]]; then
  image_refs+=("${image_name}:latest")
fi

build_args=()
build_args+=(buildx build)
build_args+=(--platform "$platform")
build_args+=(-f "$dockerfile")

for image_ref in "${image_refs[@]}"; do
  build_args+=(--tag "$image_ref")
done

build_args+=(--load)
build_args+=("$context_dir")

echo "Building agent image with docker buildx build"
echo "  working dir: ${agent_dir}"
echo "  context:     $context_dir"
echo "  dockerfile:  $dockerfile"
echo "  platform:    $platform"
echo "  output:      load"
echo "  image refs:"
for image_ref in "${image_refs[@]}"; do
  echo "    - $image_ref"
done

docker "${build_args[@]}"

echo "Done."
echo "Loaded image refs:"
for image_ref in "${image_refs[@]}"; do
  echo "  - $image_ref"
done
