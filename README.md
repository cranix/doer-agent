# doer-agent

`doer-agent`는 doer 서버에 연결되는 리버스 폴링 에이전트 런타임입니다.
로컬 머신이나 원격 워크스페이스에서 작업을 실행하고, 결과를 doer로 다시 전달합니다.

## 현재 구조

이 저장소 루트 자체가 `doer-agent` 프로젝트입니다.
예전 문서의 `agent/` 하위 디렉토리 기준 설명은 더 이상 맞지 않습니다.

주요 엔트리 포인트:

- `doer-agent`: 에이전트 본체 CLI
- `codex`: Codex 래퍼 CLI

## 요구 사항

- Node.js 20+
- doer 서버 접근 가능
- 발급된 `user-id`
- 발급된 `agent-secret`

기본 서버는 `https://doer.cranix.net`입니다.
다른 서버를 쓸 때만 `--server` 또는 `DOER_AGENT_SERVER`를 지정하면 됩니다.

## 빠른 실행

패키지 설치 없이 바로 실행하려면 `npx`를 사용합니다.
CLI는 `run start --` 없이 직접 옵션을 받습니다.

macOS / Linux:

```bash
WORKSPACE="${WORKSPACE:-$PWD}" npx -y doer-agent \
  --user-id <userId> \
  --agent-secret <SECRET>
```

PowerShell:

```powershell
$env:WORKSPACE = if ($env:WORKSPACE) { $env:WORKSPACE } else { (Get-Location).Path }
npx -y doer-agent `
  --user-id <userId> `
  --agent-secret <SECRET>
```

다른 서버를 붙일 때:

```bash
WORKSPACE="${WORKSPACE:-$PWD}" npx -y doer-agent \
  --server http://localhost:2020 \
  --user-id <userId> \
  --agent-secret <SECRET>
```

## 로컬 개발

이 저장소를 직접 수정하거나 빌드하려면 루트에서 실행합니다.

설치:

```bash
npm install
```

개발 모드 실행:

```bash
npm run start -- --user-id <userId> --agent-secret <SECRET>
```

배포 산출물 빌드:

```bash
npm run build
```

빌드 산출물 실행:

```bash
npm run start:dist -- --user-id <userId> --agent-secret <SECRET>
```

기본 서버가 아닌 경우에는 위 명령들에 `--server <url>`을 추가하면 됩니다.

## 환경변수

CLI 인자가 우선이고, 없으면 아래 환경변수를 사용합니다.

- `DOER_AGENT_SERVER`: 선택. 기본값은 `https://doer.cranix.net`
- `DOER_AGENT_USER_ID`: 필수
- `DOER_AGENT_SECRET`: 필수
- `WORKSPACE`: 선택. 작업 디렉터리
- `DOER_AGENT_MAX_CONCURRENCY`: 선택. 동시 작업 수

예시:

```bash
DOER_AGENT_USER_ID=<userId> \
DOER_AGENT_SECRET=<SECRET> \
WORKSPACE=/absolute/path/to/workspace \
npm run start
```

로컬 서버 예시:

```bash
DOER_AGENT_SERVER=http://localhost:2020 \
DOER_AGENT_USER_ID=<userId> \
DOER_AGENT_SECRET=<SECRET> \
WORKSPACE=/absolute/path/to/workspace \
npm run start
```

## 자주 쓰는 옵션

- `--server`: doer 서버 베이스 URL
- `--user-id`: doer 사용자 ID
- `--agent-secret`: doer가 발급한 에이전트 시크릿
- `--workspace-dir`: 실행 전에 이동할 작업 디렉터리

## 시크릿 발급 예시

서버가 로컬에서 돌고 있을 때 예시입니다.

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

## 참고

- `runtime/`에는 실행 보조 스크립트가 들어 있습니다.
- Playwright MCP 프록시는 에이전트 상태 디렉터리(`~/.doer-agent`) 아래 소켓을 사용합니다.
- 이 저장소에는 예전 README에 있던 `scripts/build.sh`, `scripts/publish.sh`, `docker-compose.dev.yml`이 없습니다. 현재 문서는 실제 파일 구조 기준으로 정리되어 있습니다.
