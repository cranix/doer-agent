#!/usr/bin/env bash
set -euo pipefail

IMAGE_TAG="${DOER_AGENT_IMAGE_TAG:-doer-agent:latest}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AGENT_DIR="${SCRIPT_DIR}"
WORKSPACE_DIR="$PWD"

AGENT_ID=""
FORWARD_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --agent-id)
      if [[ $# -lt 2 ]]; then
        echo "run-agent-docker.sh: --agent-id requires a value" >&2
        exit 1
      fi
      AGENT_ID="$2"
      shift 2
      ;;
    --agent-id=*)
      AGENT_ID="${1#--agent-id=}"
      shift
      ;;
    *)
      FORWARD_ARGS+=("$1")
      shift
      ;;
  esac
done

AGENT_ID="$(printf '%s' "$AGENT_ID" | xargs)"
if [[ -z "$AGENT_ID" ]]; then
  echo "run-agent-docker.sh: --agent-id is required" >&2
  exit 1
fi
if [[ "$AGENT_ID" == *"/"* ]] || [[ ! "$AGENT_ID" =~ ^[a-zA-Z0-9._:-]+$ ]]; then
  echo "run-agent-docker.sh: invalid --agent-id '$AGENT_ID'" >&2
  exit 1
fi

CONTAINER_WORKSPACE_DIR="/agent/${AGENT_ID}/workspace"

docker build -f "${AGENT_DIR}/Dockerfile" -t "${IMAGE_TAG}" "${AGENT_DIR}"

exec docker run --rm -it \
  -w "${CONTAINER_WORKSPACE_DIR}" \
  -v "${WORKSPACE_DIR}:${CONTAINER_WORKSPACE_DIR}" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  "${IMAGE_TAG}" \
  "${FORWARD_ARGS[@]}"
