# doer-agent

`agent/`는 doer의 리버스 폴링 에이전트 및 로컬 MCP 릴레이를 위한 독립 실행 디렉토리입니다.

## 준비

- doer 서버가 실행 중이어야 합니다. (기본: `http://localhost:2020`)
- 아래 명령은 `agent/` 디렉토리 기준입니다.

```bash
npm install
```

## 에이전트 실행 (고정 시크릿 + 리버스 폴링)

1. 로그인 세션에서 고정 시크릿 발급:

```bash
curl -X POST 'http://localhost:2020/api/users/<userId>/agent/secret' \
  -H 'Content-Type: application/json' \
  --cookie 'doer_session=<session-cookie>' \
  -d '{"name":"my-laptop"}'
```

응답 예시:

```json
{
  "agent": { "id": "agent_...", "name": "my-laptop" },
  "agentSecret": "<SECRET>"
}
```

2. 에이전트 실행:

```bash
npm run start -- --server http://localhost:2020 --user-id <userId> --agent-secret <SECRET>
```

3. 원격 명령 전송:

```bash
curl -X POST 'http://localhost:2020/api/users/<userId>/agent/tasks' \
  -H 'Content-Type: application/json' \
  --cookie 'doer_session=<session-cookie>' \
  -d '{"agentId":"agent_...","command":"pwd && ls -la"}'
```

## Docker 실행

`agent/` 디렉토리에서:

```bash
./bin/run-agent-docker.sh --server http://<doer-host>:2020 --user-id <userId> --agent-secret <SECRET>
```

- 컨테이너 내부에서 `docker` 명령이 필요하면 Docker socket 마운트가 필요합니다.

## 로컬 MCP 릴레이 실행

1. 릴레이 시크릿 발급:

```bash
curl -X POST 'http://localhost:2020/api/users/<userId>/mcp-relay/secret' \
  -H 'Content-Type: application/json' \
  --cookie 'doer_session=<session-cookie>' \
  -d '{"label":"my-mcp-relay"}'
```

2. 로컬 config 파일 작성 (`local-mcp-relay.config.json`):

```json
{
  "servers": [
    {
      "serverKey": "playwright",
      "name": "Playwright MCP",
      "command": "npx",
      "args": ["-y", "@playwright/mcp"]
    }
  ]
}
```

3. 릴레이 실행:

```bash
npm run relay:mcp -- --server http://localhost:2020 --user-id <userId> --relay-secret <SECRET> --config ./local-mcp-relay.config.json
```

4. doer MCP tool 호출 시 동적 툴 이름:

- `local_<serverKey>__<toolName>`
