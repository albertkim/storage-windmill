import { S3Client } from "bun"
import * as wmill from "windmill-client"

// All Canada provinces/territories + US states + DC (ISO 3166-2 codes)
const ALL_COUNTRY_STATE_COMBOS: { combo: string; name: string }[] = [
  { combo: "ca-ab", name: "Alberta" },
  { combo: "ca-bc", name: "British Columbia" },
  { combo: "ca-mb", name: "Manitoba" },
  { combo: "ca-nb", name: "New Brunswick" },
  { combo: "ca-nl", name: "Newfoundland and Labrador" },
  { combo: "ca-ns", name: "Nova Scotia" },
  { combo: "ca-nt", name: "Northwest Territories" },
  { combo: "ca-nu", name: "Nunavut" },
  { combo: "ca-on", name: "Ontario" },
  { combo: "ca-pe", name: "Prince Edward Island" },
  { combo: "ca-qc", name: "Quebec" },
  { combo: "ca-sk", name: "Saskatchewan" },
  { combo: "ca-yt", name: "Yukon" },
  { combo: "us-al", name: "Alabama" },
  { combo: "us-ak", name: "Alaska" },
  { combo: "us-az", name: "Arizona" },
  { combo: "us-ar", name: "Arkansas" },
  { combo: "us-ca", name: "California" },
  { combo: "us-co", name: "Colorado" },
  { combo: "us-ct", name: "Connecticut" },
  { combo: "us-de", name: "Delaware" },
  { combo: "us-dc", name: "District of Columbia" },
  { combo: "us-fl", name: "Florida" },
  { combo: "us-ga", name: "Georgia" },
  { combo: "us-hi", name: "Hawaii" },
  { combo: "us-id", name: "Idaho" },
  { combo: "us-il", name: "Illinois" },
  { combo: "us-in", name: "Indiana" },
  { combo: "us-ia", name: "Iowa" },
  { combo: "us-ks", name: "Kansas" },
  { combo: "us-ky", name: "Kentucky" },
  { combo: "us-la", name: "Louisiana" },
  { combo: "us-me", name: "Maine" },
  { combo: "us-md", name: "Maryland" },
  { combo: "us-ma", name: "Massachusetts" },
  { combo: "us-mi", name: "Michigan" },
  { combo: "us-mn", name: "Minnesota" },
  { combo: "us-ms", name: "Mississippi" },
  { combo: "us-mo", name: "Missouri" },
  { combo: "us-mt", name: "Montana" },
  { combo: "us-ne", name: "Nebraska" },
  { combo: "us-nv", name: "Nevada" },
  { combo: "us-nh", name: "New Hampshire" },
  { combo: "us-nj", name: "New Jersey" },
  { combo: "us-nm", name: "New Mexico" },
  { combo: "us-ny", name: "New York" },
  { combo: "us-nc", name: "North Carolina" },
  { combo: "us-nd", name: "North Dakota" },
  { combo: "us-oh", name: "Ohio" },
  { combo: "us-ok", name: "Oklahoma" },
  { combo: "us-or", name: "Oregon" },
  { combo: "us-pa", name: "Pennsylvania" },
  { combo: "us-ri", name: "Rhode Island" },
  { combo: "us-sc", name: "South Carolina" },
  { combo: "us-sd", name: "South Dakota" },
  { combo: "us-tn", name: "Tennessee" },
  { combo: "us-tx", name: "Texas" },
  { combo: "us-ut", name: "Utah" },
  { combo: "us-vt", name: "Vermont" },
  { combo: "us-va", name: "Virginia" },
  { combo: "us-wa", name: "Washington" },
  { combo: "us-wv", name: "West Virginia" },
  { combo: "us-wi", name: "Wisconsin" },
  { combo: "us-wy", name: "Wyoming" }
]

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
    prefix: "facility-scraper/apify/"
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

  const entries = Array.from(latestByCountryState.entries()).map(([combo, data]) => ({
    combo,
    ...data
  }))

  const inS3 = new Set(latestByCountryState.keys())
  const missing = ALL_COUNTRY_STATE_COMBOS.filter((item) => !inS3.has(item.combo))

  return {
    latestByCountryState: Object.fromEntries(latestByCountryState),
    missing: missing.map((item) => item.name)
  }
}
