import { S3Client } from "bun"
import * as wmill from "windmill-client"
import { ALL_COUNTRY_STATE_COMBOS } from "./country_state_combos"

const S3_PREFIX = "facility-scraper/apify/"

export async function main() {
  const AWS_ACCESS_KEY_ID = await wmill.getVariable("f/secrets/aws_access_key_id")
  const AWS_SECRET_ACCESS_KEY = await wmill.getVariable("f/secrets/aws_secret_access_key")
  const AWS_REGION = await wmill.getVariable("f/secrets/aws_region")
  const S3_BUCKET = await wmill.getVariable("f/secrets/s3_bucket")

  // List files from S3 bucket (Bun native S3 API)
  const s3Client = new S3Client({
    accessKeyId: AWS_ACCESS_KEY_ID,
    secretAccessKey: AWS_SECRET_ACCESS_KEY,
    bucket: S3_BUCKET,
    region: AWS_REGION
  })

  // List all files (with pagination)
  const files: { key: string }[] = []
  let listOpts: { prefix: string; maxKeys?: number; startAfter?: string } = {
    prefix: S3_PREFIX
  }
  do {
    const result = await s3Client.list(listOpts)
    files.push(...(result.contents ?? []))
    if (result.isTruncated && result.contents?.length) {
      const lastKey = result.contents.at(-1)!.key
      listOpts = { ...listOpts, startAfter: lastKey }
    } else {
      break
    }
  } while (true)

  // Keys are like facility-scraper/apify/ca-ab-2026-01-19.json (country-state-YYYY-MM-DD)
  const latestByCountryState = new Map<string, { latestDate: string; key: string }>()

  for (const file of files) {
    const basename =
      file.key
        .split("/")
        .pop()
        ?.replace(/\.\w+$/, "") ?? ""
    const parts = basename.split("-")
    if (parts.length < 5) continue // need country, state, yyyy, mm, dd
    const country = parts[0].toLowerCase()
    const state = parts[1].toLowerCase()
    const date = `${parts[2]}-${parts[3]}-${parts[4]}`
    const combo = `${country}-${state}`

    const existing = latestByCountryState.get(combo)
    if (!existing || date > existing.latestDate) {
      latestByCountryState.set(combo, { latestDate: date, key: file.key })
    }
  }

  const inS3 = new Set(latestByCountryState.keys())
  const missing = ALL_COUNTRY_STATE_COMBOS.filter((item) => !inS3.has(item.combo))

  return {
    latestByCountryState: Object.fromEntries(latestByCountryState),
    missing: missing.map((item) => item.name)
  }
}
