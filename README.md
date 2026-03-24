# doer-agent

`agent/`는 doer의 리버스 폴링 에이전트를 위한 독립 실행 디렉토리입니다.

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

3. 원격 Codex 실행 전송:

```bash
curl -X POST 'http://localhost:2020/api/users/<userId>/agent/codex-tasks' \
  -H 'Content-Type: application/json' \
  --cookie 'doer_session=<session-cookie>' \
  -d '{"agentId":"agent_...","prompt":"현재 작업 디렉토리와 파일 목록을 요약해줘"}'
```

## Docker로 기본 실행

기본 실행은 퍼블릭 이미지 `cranix/doer-agent:latest`를 직접 사용합니다.

```bash
docker run --rm -it \
  -v "${PWD}:/workspace" \
  cranix/doer-agent:latest \
  --server http://<doer-host>:2020 \
  --user-id <userId> \
  --agent-secret <SECRET>
```

PowerShell:

```powershell
docker run --rm -it `
  -v "${PWD}:/workspace" `
  cranix/doer-agent:latest `
  --server http://<doer-host>:2020 `
  --user-id <userId> `
  --agent-secret <SECRET>
```

- 다른 이미지를 쓰려면 `cranix/doer-agent:latest` 부분만 원하는 태그로 바꾸면 됩니다.
- 기본 실행은 Docker socket을 마운트하지 않습니다.
- 따라서 컨테이너 내부의 중첩 `docker` 사용은 기본 실행 범위에 포함되지 않습니다.

## Agent 이미지 빌드/퍼블리시

배포용 agent 이미지는 `agent/` 디렉토리에서 `build`와 `publish` 스크립트로 분리해 사용합니다. 두 스크립트 모두 `.` 컨텍스트와 `./Dockerfile`을 기본값으로 사용하며, 기본 이미지 리포지토리는 `cranix/doer-agent`입니다.

로컬 빌드(load):

```bash
./scripts/build.sh
./scripts/build.sh --tag v1.2.3 --also-latest
./scripts/build.sh --image docker.io/example/doer-agent --platform linux/amd64
```

레지스트리 퍼블리시(push):

```bash
./scripts/publish.sh
./scripts/publish.sh --tag v1.2.3
./scripts/publish.sh --image docker.io/example/doer-agent --tag v1.2.3
```

`publish.sh`는 태그 기반 퍼블리시 시 `:<tag>`와 `:latest`를 함께 push합니다.

퍼블리시한 이미지는 직접 `docker run`으로 지정해 사용합니다.

```bash
docker run --rm -it \
  -v "${PWD}:/workspace" \
  ghcr.io/example/doer-agent:v1.2.3 \
  --server http://<doer-host>:2020 \
  --user-id <userId> \
  --agent-secret <SECRET>
```

## Docker Compose로 개발 실행

개발 실행은 `agent/docker-compose.yml` 기준이며, **agent 컨테이너 내부에서 dockerd를 직접 기동하는 단일 컨테이너 방식**입니다.

```bash
export DOER_AGENT_SERVER=http://<doer-host>:2020
export DOER_AGENT_USER_ID=<userId>
export DOER_AGENT_SECRET=<SECRET>
# 선택: 다른 작업 디렉토리를 마운트하려면 지정
# export DOER_AGENT_WORKSPACE=/absolute/path/to/workspace

cd agent
docker compose up --build agent-dev
```

백그라운드 실행:

```bash
cd agent
docker compose up --build -d agent-dev
docker compose logs -f agent-dev
```

정리:

```bash
cd agent
docker compose down
```

- `DOER_AGENT_SERVER`, `DOER_AGENT_USER_ID`, `DOER_AGENT_SECRET`는 필수입니다.
- `DOER_AGENT_WORKSPACE`를 지정하지 않으면 기본값으로 저장소 루트(`..`)가 `/workspace`에 마운트됩니다.
- Docker 데이터는 볼륨 `doer_agent_dind_data`에 저장됩니다.
- 이 모드는 `privileged: true`가 필요합니다.

## Windows 권장 사항

- Windows에서는 Docker Desktop의 `Linux containers` 모드가 필요합니다.
- 로컬 개발 환경은 Docker Desktop의 WSL2 백엔드를 권장합니다.
- Windows 네이티브 PowerShell보다 WSL2 셸에서 직접 `docker compose`를 실행하는 쪽이 더 직접적일 수 있습니다.
- PowerShell에서 `/var/run/docker.sock` 마운트 가능 여부는 Docker Desktop 설정과 실행 환경에 따라 다릅니다.
