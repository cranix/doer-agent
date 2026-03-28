#!/bin/sh
export DOER_MCP_SOCKET='/Users/nick/.doer-agent/playwright-mcp-daemon/playwright-mcp.sock'
exec '/Users/nick/Documents/cranixproject/doer/agent/runtime/bin/doer-mcp-proxy' "$@"
