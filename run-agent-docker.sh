#!/usr/bin/env bash
set -euo pipefail

IMAGE_TAG="${DOER_AGENT_IMAGE_TAG:-doer-agent:latest}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AGENT_DIR="${SCRIPT_DIR}"
WORKSPACE_DIR="$PWD"

docker build -f "${AGENT_DIR}/Dockerfile" -t "${IMAGE_TAG}" "${AGENT_DIR}"

exec docker run --rm -it \
  -w /workspace \
  -v "${WORKSPACE_DIR}":/workspace \
  -v /var/run/docker.sock:/var/run/docker.sock \
  "${IMAGE_TAG}" \
  "$@"
