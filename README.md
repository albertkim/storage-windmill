# Windmill Local Dev (Self-Hosted)

This repo is set up for minimal local Windmill development with a TypeScript
script managed through Windmill sync.

## What is included

- `docker-compose.yml`, `.env`, `Caddyfile`: official self-host stack files
- `wmill.yaml`: Windmill sync config
- `dependencies/package.json`: workspace TypeScript dependencies (Windmill)
- `f/...`: Scripts and yml metadata
- `scripts/windmill-up.sh`: starts local Windmill stack
- `scripts/windmill-push.sh`: pushes repo content to Windmill workspace
- `package.json`: npm scripts for common Windmill commands

## 1) Start Windmill locally

```bash
chmod +x scripts/windmill-up.sh scripts/windmill-push.sh
bun wm:up
```

Open `http://localhost` and sign in:

- email: `admin@windmill.dev` (default from Windmill)
- password: `changeme` (default from Windmill)

## 2) Create an API token

In Windmill UI:

1. Open user settings.
2. Create a token.
3. Copy it.

## 3) Push local scripts to Windmill

Use `.env` as the single config file for:

- Docker runtime config (`DATABASE_URL`, log settings)
- Windmill push config (`WMILL_TOKEN`, `WMILL_BASE_URL`, `WMILL_WORKSPACE`)
- Runtime variables with `SECRET_` prefix

Then run:

```bash
bun wm:push
```

After push, run script path `f/hello/hello_world` in Windmill. It logs
`Hello, world!` and returns `{ "message": "Hello, world!" }`.

For scripts using workspace deps, annotate script files (example):

```ts
// package_json: default
import dayjs from "dayjs"
```

Useful commands:

```bash
bun wm:logs
bun wm:down
```

## Secrets management

- Put shared runtime variables in `.env` with `SECRET_` prefix.
- On `bun wm:push`, each `SECRET_*` entry syncs to:
  - `f/secrets/<key_lowercase>`
- Example:
  - `SECRET_APIFY_API_TOKEN=...` -> `f/secrets/apify_api_token`
