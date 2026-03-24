#!/usr/bin/env bash
set -euo pipefail

export DOCKER_TLS_CERTDIR=""
mkdir -p /var/lib/docker /var/run

start_dockerd() {
  local driver="$1"
  rm -f /var/run/docker.pid
  dockerd \
    --host=unix:///var/run/docker.sock \
    --storage-driver="${driver}" \
    >/tmp/dockerd.log 2>&1 &
  DOCKERD_PID=$!
}

wait_docker_ready() {
  local timeout="$1"
  for _ in $(seq 1 "${timeout}"); do
    if docker info >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

start_dockerd overlay2
if ! wait_docker_ready 20; then
  echo "[agent-entrypoint] overlay2 start failed, fallback to vfs" >&2
  kill "${DOCKERD_PID}" >/dev/null 2>&1 || true
  wait "${DOCKERD_PID}" >/dev/null 2>&1 || true

  start_dockerd vfs
  if ! wait_docker_ready 40; then
    echo "[agent-entrypoint] dockerd failed to become ready" >&2
    tail -n 200 /tmp/dockerd.log >&2 || true
    exit 1
  fi
fi

exec /app/node_modules/.bin/tsx /app/src/agent.ts "$@"
