# storage-windmill

Windmill workspace repo for a self-storage data pipeline.

The project runs an A-G flow that:

1. checks country/state scrape coverage,
2. starts Apify Google Maps scrapes,
3. uploads missing Apify run outputs to S3,
4. cleans raw records,
5. scrapes emails from site sitemaps/pages,
6. builds a gold dataset,
7. builds a platinum dataset grouped by domain.

## Repository layout

- `docker-compose.yml`, `.env`, `Caddyfile`: local Windmill self-host stack config
- `wmill.yaml`: Windmill sync config
- `dependencies/package.json`: workspace dependency bundle for Windmill scripts
- `f/facility_scraper_flow/*`: scripts + flow definitions
- `scripts/windmill-up.sh`: start local Windmill stack
- `scripts/windmill-push.sh`: sync `.env` secrets + push scripts/flows to Windmill
- `package.json`: convenience commands

## Local setup

1. Make scripts executable:

```bash
chmod +x scripts/windmill-up.sh scripts/windmill-push.sh
```

2. Start services:

```bash
bun wm:up
```

3. Open `http://localhost` and sign in with Windmill defaults:

- email: `admin@windmill.dev`
- password: `changeme`

4. Create a Windmill API token in user settings.

5. Put token and config in `.env`.

## Required .env values

- `WMILL_TOKEN`: Windmill API token (required for `bun wm:push`)
- `WMILL_BASE_URL`: defaults to `http://localhost`
- `WMILL_WORKSPACE`: defaults to `starter`
- `DATABASE_URL`: used by Docker services

Runtime secrets for scripts should be set as `SECRET_*` env vars in `.env`.
On `bun wm:push`, they are upserted to Windmill variables under `f/secrets/*`.

Example mappings:

- `SECRET_APIFY_API_TOKEN` -> `f/secrets/apify_api_token`
- `SECRET_AWS_ACCESS_KEY_ID` -> `f/secrets/aws_access_key_id`
- `SECRET_AWS_SECRET_ACCESS_KEY` -> `f/secrets/aws_secret_access_key`
- `SECRET_AWS_REGION` -> `f/secrets/aws_region`
- `SECRET_S3_BUCKET` -> `f/secrets/s3_bucket`

## Push flow/scripts to Windmill

```bash
bun wm:push
```

This command does two things:

1. upserts `SECRET_*` values from `.env` to Windmill variables,
2. runs `windmill-cli sync push` for repo content (using `wmill.yaml`).

## Main flow

Flow path:

- `f/facility_scraper_flow/facility_scraper_pipeline`

Steps:

- `A` `a_apify_scrape_detect`
- `B` `b_apify_scrape`
- `C` `c_apify_consolidate_runs`
- `D` `d_apify_clean`
- `E` `e_sitemap_email_scrape`
- `F` `f_apify_gold`
- `G` `g_apify_platinum`

### Current step-B behavior

`b_apify_scrape` now takes only:

- `stateName`

It no longer accepts user input for search term or max crawl count.

- Search term is hardcoded to `self storage`.
- `maxCrawledPlacesPerSearch` is derived from `desiredScrapeCount` in `f/facility_scraper_flow/country_state_combos.ts`.

## Useful commands

```bash
bun wm:logs
bun wm:down
bun env:encrypt
bun env:decrypt
```
