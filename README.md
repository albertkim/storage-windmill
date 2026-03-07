# Windmill Local Dev (Self-Hosted)

This repo is set up for minimal local Windmill development with a TypeScript
script managed through Windmill sync.

## What is included

- `docker-compose.yml`, `.env`, `Caddyfile`: official self-host stack files
- `wmill.yaml`: Windmill sync config
- `dependencies/package.json`: workspace TypeScript dependencies (Windmill)
- `f/hello/hello_world.ts`: TypeScript hello-world script
- `f/hello/hello_world.script.yaml`: script metadata
- `scripts/windmill-up.sh`: starts local Windmill stack
- `scripts/windmill-push.sh`: pushes repo content to Windmill workspace
- `package.json`: npm scripts for common Windmill commands

## 1) Start Windmill locally

```bash
chmod +x scripts/windmill-up.sh scripts/windmill-push.sh
bun wm:up
```

Open `http://localhost` and sign in:

- email: `admin@windmill.dev`
- password: `changeme`

## 2) Create an API token

In Windmill UI:

1. Open user settings.
2. Create a token.
3. Copy it.

## 3) Push local scripts to Windmill

Preferred: use `.env.local` (not committed):

```bash
cp .env.local.example .env.local
# edit .env.local and set WMILL_TOKEN
npm run wm:push
```

Alternative: export values in your shell:

```bash
export WMILL_TOKEN='paste-your-token'
export WMILL_BASE_URL='http://localhost'
export WMILL_WORKSPACE='starter'
npm run wm:push
```

After push, run script path `f/hello/hello_world` in Windmill. It logs
`Hello, world!` and returns `{ "message": "Hello, world!" }`.

For scripts using workspace deps, annotate script files (example):

```ts
// package_json: default
import dayjs from "dayjs";
```

Useful commands:

```bash
npm run wm:logs
npm run wm:down
```
