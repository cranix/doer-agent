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

- `runtime/`에는 Git 인증 등 실행 보조 스크립트가 들어 있습니다.
- Playwright MCP 프록시는 에이전트 상태 디렉터리(`~/.doer-agent`) 아래 소켓을 사용합니다.
- 이 저장소에는 예전 README에 있던 `scripts/build.sh`, `scripts/publish.sh`, `docker-compose.dev.yml`이 없습니다. 현재 문서는 실제 파일 구조 기준으로 정리되어 있습니다.

## Browser login and saved accounts

The built-in `doer_browser` MCP server can hand an existing Chromium tab to the
signed-in Doer user. In Doer, select the same agent and open **Browser login**.
The user can fill top-level login fields, click an updated browser preview, use
Tab/Enter, and finish additional authentication from their phone. Click **Confirm
login · Resume** only after checking the intended account. The waiting agent must
then verify authentication before continuing the original task.

Requirements:

- Both Doer web and doer-agent must include this feature; restart the agent after
  updating it so the new MCP tools are registered.
- Chrome must already run on the Mac with a loopback CDP endpoint. The default is
  `http://127.0.0.1:9222`; set `DOER_BROWSER_CDP_URL` before starting doer-agent to
  use another loopback HTTP port. Do not expose the debugging port publicly.
  This feature never launches, closes, or migrates the user's Chrome profile.
- Open Doer over HTTPS on the phone (localhost is supported for development).
  Set the web server's `DOER_BASE_URL` to its public origin for CSRF validation.
- The Mac and doer-agent remain running. This does not unlock macOS/Keychain or
  automate Touch ID, device-bound passkeys, or OS-level authentication dialogs.

Tools: `browser_login_tabs`, `browser_login_saved`, `browser_login_request`, `browser_login_wait`.
When login is needed, first try `browser_login_saved` on the login tab. It only
uses accounts explicitly enabled for automatic login at that exact origin.
Unambiguous, top-level username/password POST forms with a login submit button
are supported. Verify the intended account after submission; submission is not
proof of authentication. Unsupported forms, multiple accounts, additional
verification and failed attempts need a human handoff. Do not retry a failed
password; the broker limits each account/tab attempt to once per five minutes.
For a handoff, request it using the tab ID and keep calling the wait
tool while pending. Stop browser automation and inspection during the handoff.
Requests expire after 15 minutes, are scoped to one tab, survive NATS reconnects,
and are discarded on process restart. Cancel/expiry must not resume authenticated
work. A disconnected tab cancels its request. Login need is identified by the
calling agent; there is no background login watcher.

In **Browser login → Saved logins**, users can add an account, edit its username
or replace its password, delete it, and enable/disable automatic login. A blank
password on edit keeps the existing password; passwords are never read back to
the UI. To change a login origin, create a new account. During a human handoff,
users can also explicitly save a username/password pair or fill a saved account.
Ordinary one-time input and OTPs are not saved. Saving does not imply consent to
automatic login; that permission is a separate checkbox.

The vault is **local to the agent**, not in the Doer database. The default root
is `~/.doer-agent/browser-vault` (`DOER_BROWSER_VAULT_DIR` overrides the root).
Each server/user/agent/CDP-endpoint combination has a separate directory. AES-256-GCM
encrypts all entries; the local key and encrypted file use owner-only permissions.
The vault survives agent restarts without a Keychain popup. Only run one agent
process for a vault directory. The configured CDP endpoint identifies the browser;
use a separate endpoint/agent for another browser profile. This does not import
Chrome/Apple Passwords or sync credentials to another machine.

Security boundaries:

- Human input uses a session-only, same-origin, no-store endpoint. Agent bearer
  tokens can request/wait but cannot fetch login previews or complete a handoff.
- The client encrypts each command with AES-256-GCM and an ephemeral RSA-OAEP key
  owned by the running agent. Authentication binds it to the request and a
  single-use screen revision. The web server/NATS relay does not receive plaintext
  input. This is not protection against a compromised web client/server replacing
  the delivered code or public key.
- The broker checks the exact document and origin before input; cross-origin form
  submissions are rejected. Only top-level HTML input fields are supported in this
  first version. Embedded frames are masked in previews; use a top-level provider
  login tab if the site offers one. Popups require a separate handoff for that tab.
- Credentials are persisted only after explicit user choice. There is no screenshot
  file, trace, page console capture or secret in model tool results. Browser login sessions remain in the existing
  Chrome profile, following that site's rules. Inputs filled by the broker are
  cleared best-effort when a request ends; JavaScript/Node strings cannot provide
  guaranteed memory zeroization. Screenshots are available only to the user and
  can still contain sensitive page content outside masked input/frame elements.
- **This is an application-level handoff, not an OS sandbox.** The current
  unrestricted agent shell, independent browser MCP tools, or another process
  running as the same Mac user can bypass it or inspect Chrome. Pausing other
  tools is cooperative. Strong isolation requires a separate restricted browser
  service/account or VM with exclusive CDP ownership. The vault key is on the same
  computer as the encrypted file; encryption protects against accidental plaintext
  exposure, not compromise of that OS account. The saved-login tool is a controlled
  capability and does not return passwords, but unrestricted shell/CDP access can
  bypass that boundary.

Verification:

```sh
npm run build
npm test
# Includes real Chromium tests for navigation/replay/expiry and multi-step login:
DOER_BROWSER_TEST_EXECUTABLE='/Applications/Google Chrome.app/Contents/MacOS/Google Chrome' npm test
```

For an end-to-end check, use a disposable local login page, request its tab via the
MCP tool, enter synthetic credentials and an OTP from Doer at mobile width, then
confirm completion. Verify that tool results contain only request metadata,
commands sent to Doer contain encrypted envelopes, and the original tab retains
its authenticated session. Do not record real credentials in test traces.

## npm publishing without local Keychain approval

`.github/workflows/publish.yml` publishes from GitHub-hosted runners using npm
Trusted Publishing (OIDC). It stores no npm publishing password or long-lived
publishing token. A release must be explicitly dispatched on `develop` with the
exact package version and commit SHA. Every release runs the build and all tests,
including real Chromium tests, before publishing.

One-time npm package setup (requires the owner's npm authentication):

- Package: `doer-agent`
- Provider: GitHub Actions
- Organization/user: `cranix`
- Repository: `doer-agent`
- Workflow filename: `publish.yml`
- Allow direct `npm publish` (stage-only permission still requires manual approval)

After connecting the trusted publisher, commit and push the desired release, then:

```sh
gh workflow run publish.yml --repo cranix/doer-agent --ref develop \
  -f version="$(node -p 'require("./package.json").version')" \
  -f commit="$(git rev-parse HEAD)"
```

Monitor the Actions run and verify the npm version and `gitHead` before reporting
success. Do not restart the user's running agent as part of package publication.
See https://docs.npmjs.com/trusted-publishers/ for the npm configuration details.
