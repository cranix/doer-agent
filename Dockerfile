FROM node:22-bookworm AS playwright-browsers

ARG PW_PLAYWRIGHT_CORE_VERSION=1.59.0-alpha-1771104257000
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV PLAYWRIGHT_SKIP_BROWSER_GC=1

WORKDIR /pw

RUN npm init -y >/dev/null 2>&1 && \
  npm i --no-save \
  pw@npm:playwright-core@${PW_PLAYWRIGHT_CORE_VERSION}

RUN ARCH="$(uname -m)" \
  && if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then \
      node node_modules/pw/cli.js install chromium; \
    else \
      node node_modules/pw/cli.js install chromium chrome; \
    fi

FROM node:22-bookworm

WORKDIR /app

ARG TERRAFORM_APT_SOURCE=/etc/apt/sources.list.d/hashicorp.list
ARG DOCKER_APT_SOURCE=/etc/apt/sources.list.d/docker.sources

RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  bsdextrautils \
  ca-certificates \
  gh \
  git \
  jq \
  awscli \
  poppler-utils \
  python-is-python3 \
  python3-pip \
  python3-venv \
  python3 \
  ripgrep \
  wget \
  xdg-utils \
  fonts-noto-color-emoji \
  fonts-noto-cjk \
  fonts-nanum \
  fontconfig \
  libnspr4 \
  libnss3 \
  libatk1.0-0 \
  libatk-bridge2.0-0 \
  libdbus-1-3 \
  libcups2 \
  libxkbcommon0 \
  libatspi2.0-0 \
  libxcomposite1 \
  libxdamage1 \
  libxfixes3 \
  libxrandr2 \
  libgbm1 \
  libasound2 \
  libprotobuf32 \
  gpg \
  && rm -rf /var/lib/apt/lists/*

RUN install -m 0755 -d /etc/apt/keyrings && \
  wget -qO /etc/apt/keyrings/docker.asc https://download.docker.com/linux/debian/gpg && \
  chmod a+r /etc/apt/keyrings/docker.asc && \
  printf 'Types: deb\nURIs: https://download.docker.com/linux/debian\nSuites: bookworm\nComponents: stable\nSigned-By: /etc/apt/keyrings/docker.asc\n' > "${DOCKER_APT_SOURCE}" && \
  apt-get update && \
  apt-get install -y --no-install-recommends \
  docker-ce-cli \
  docker-buildx-plugin \
  docker-compose-plugin \
  && docker --version >/dev/null && \
  docker buildx version >/dev/null && \
  docker compose version >/dev/null && \
  rm -rf /var/lib/apt/lists/*

RUN wget -O- https://apt.releases.hashicorp.com/gpg | gpg --dearmor > /usr/share/keyrings/hashicorp-archive-keyring.gpg && \
  echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com bookworm main" > "${TERRAFORM_APT_SOURCE}" && \
  apt-get update && \
  apt-get install -y --no-install-recommends terraform && \
  terraform version >/dev/null && \
  rm -rf /var/lib/apt/lists/*

RUN python -m pip install --break-system-packages --no-cache-dir \
  beautifulsoup4 \
  lxml \
  matplotlib \
  numpy \
  openpyxl \
  pandas \
  pillow \
  pyyaml \
  requests \
  scipy \
  tqdm

RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  xxd \
  && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json ./
RUN npm install --no-audit --no-fund
RUN /app/node_modules/.bin/codex --version >/dev/null

ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV PLAYWRIGHT_SKIP_BROWSER_GC=1
COPY --from=playwright-browsers /ms-playwright /ms-playwright

RUN set -eux; \
  ARCH="$(uname -m)"; \
  test -d "${PLAYWRIGHT_BROWSERS_PATH}"; \
  node node_modules/playwright-core/cli.js --version; \
  find "${PLAYWRIGHT_BROWSERS_PATH}" -maxdepth 1 -mindepth 1 -type d | tee /tmp/pw-list.txt; \
  if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then \
    grep -Eiq '/chromium-[0-9]+' /tmp/pw-list.txt; \
  else \
    grep -Eiq '/chromium-[0-9]+' /tmp/pw-list.txt; \
    grep -Eiq '/chrome-[^/]+' /tmp/pw-list.txt; \
  fi

COPY src/*.ts ./src/
COPY tsconfig.json ./tsconfig.json
COPY runtime ./.runtime
RUN chown -R root:root /app/.runtime && \
  chmod -R 0555 /app/.runtime

ENV NODE_ENV=production
ENV CODEX_HOME=/root/.codex

# RUN mkdir -p /workspace
# WORKDIR /workspace

ENTRYPOINT ["/app/node_modules/.bin/tsx", "/app/src/agent.ts"]
