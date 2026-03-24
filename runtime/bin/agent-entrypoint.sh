#!/usr/bin/env bash
set -euo pipefail

if [ "${DOER_AGENT_ENABLE_DIND:-1}" = "1" ]; then
  export DOCKER_TLS_CERTDIR=""
  mkdir -p /var/lib/docker /var/run

  if ! pgrep -x dockerd >/dev/null 2>&1; then
    dockerd \
      --host=unix:///var/run/docker.sock \
      --storage-driver="${DOER_AGENT_DOCKER_DRIVER:-overlay2}" \
      >/tmp/dockerd.log 2>&1 &
  fi

  for _ in $(seq 1 60); do
    if docker info >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done

  if ! docker info >/dev/null 2>&1; then
    echo "[agent-entrypoint] dockerd failed to become ready" >&2
    tail -n 200 /tmp/dockerd.log >&2 || true
    exit 1
  fi
fi

exec /app/node_modules/.bin/tsx /app/src/agent.ts "$@"
