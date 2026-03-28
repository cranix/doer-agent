#!/bin/sh
set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ -z "${DOER_MCP_SOCKET:-}" ]; then
  if [ -n "${DOER_AGENT_STATE_DIR:-}" ]; then
    DOER_MCP_SOCKET="$DOER_AGENT_STATE_DIR/playwright-mcp-daemon/playwright-mcp.sock"
  elif [ -n "${HOME:-}" ]; then
    DOER_MCP_SOCKET="$HOME/.doer-agent/playwright-mcp-daemon/playwright-mcp.sock"
  else
    echo "playwright-mcp-proxy-launcher.sh: DOER_MCP_SOCKET is not set and neither DOER_AGENT_STATE_DIR nor HOME is available" >&2
    exit 1
  fi
fi
export DOER_MCP_SOCKET
exec "$SCRIPT_DIR/doer-mcp-proxy" "$@"
