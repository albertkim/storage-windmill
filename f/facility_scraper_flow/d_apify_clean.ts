import { S3Client } from "bun"
import * as wmill from "windmill-client"

const SOURCE_PREFIX = "facility-scraper/apify/"
const CLEANED_PREFIX = "facility-scraper/apify-cleaned/"
const OUTPUT_KEY = `${CLEANED_PREFIX}cleaned.json`

const REQUIRED_NAME_KEYWORDS = ["storage"]
const REQUIRED_CATEGORY_KEYWORDS = ["storage"]
const BLOCKED_CATEGORY_KEYWORDS = [
  "groceries",
  "grocery",
  "general",
  "market",
  "store",
  "dollar",
  "pet",
  "cosmetic",
  "beauty",
  "drug",
  "pharmacy",
  "electronics",
  "convenience",
  "hotel",
  "motel",
  "photo"
]

type RawRow = Record<string, unknown>

type SilverRow = {
  googleMapsPlaceId: string
  name: string
  websiteUrlFull: string
  websiteUrlDomain: string
  reviewScore: number | null
  reviewCount: number | null
  address: string | null
  city: string | null
  state: string | null
  countryCode: string | null
  phone: string | null
}

const toNullableString = (v: unknown): string | null => (typeof v === "string" && v.trim() ? v.trim() : null)
const toNullableNumber = (v: unknown): number | null => (typeof v === "number" && Number.isFinite(v) ? v : null)

const parseComboAndDateFromKey = (key: string): { combo: string; date: string } | null => {
  const basename =
    key
      .split("/")
      .pop()
      ?.replace(/\.\w+$/, "") ?? ""
  const parts = basename.split("-")
  if (parts.length < 5) return null
  const combo = `${parts[0].toLowerCase()}-${parts[1].toLowerCase()}`
  const date = `${parts[2]}-${parts[3]}-${parts[4]}`
  return { combo, date }
}

const fileRegionFromCombo = (combo: string): { countryCode: string | null; state: string | null } => {
  const [country, state] = combo.split("-")
  return {
    countryCode: country ? country.toUpperCase() : null,
    state: state ? state.toUpperCase() : null
  }
}

const processRawRow = (
  row: RawRow,
  fileRegion: { countryCode: string | null; state: string | null }
): SilverRow | null => {
  const rawCategoryText = Array.isArray(row.categories)
    ? row.categories.filter((item) => typeof item === "string").join(" ")
    : typeof row.categoryName === "string"
      ? row.categoryName
      : ""

  const normalizedCategoryText = rawCategoryText.toLowerCase()
  const rawTitleText = typeof row.title === "string" ? row.title : ""
  const normalizedTitleText = rawTitleText.toLowerCase()

  const hasNameKeyword = REQUIRED_NAME_KEYWORDS.some((keyword) => normalizedTitleText.includes(keyword))
  const hasCategoryKeyword = REQUIRED_CATEGORY_KEYWORDS.some((keyword) => normalizedCategoryText.includes(keyword))
  const hasBlockedCategoryKeyword = BLOCKED_CATEGORY_KEYWORDS.some((keyword) =>
    normalizedCategoryText.includes(keyword)
  )
  if (hasBlockedCategoryKeyword) return null
  if (!hasNameKeyword && !hasCategoryKeyword) return null

  const title = toNullableString(row.title)
  const url = toNullableString(row.url)
  const websiteRaw = toNullableString(row.website)
  if (!title || !url || !websiteRaw) return null

  let googleMapsPlaceId = ""
  let websiteUrlFull = ""
  let websiteUrlDomain = ""

  try {
    googleMapsPlaceId = new URL(url).searchParams.get("query_place_id") ?? ""
    const httpIndex = websiteRaw.indexOf("http")
    websiteUrlFull = httpIndex >= 0 ? websiteRaw.slice(httpIndex) : websiteRaw
    websiteUrlDomain = new URL(websiteUrlFull).hostname
  } catch {
    return null
  }

  const stateRaw = toNullableString(row.state)
  const countryCodeRaw = toNullableString(row.countryCode)

  return {
    googleMapsPlaceId,
    name: title,
    websiteUrlFull,
    websiteUrlDomain,
    reviewScore: toNullableNumber(row.totalScore),
    reviewCount: toNullableNumber(row.reviewsCount),
    address: toNullableString(row.street),
    city: toNullableString(row.city),
    state: fileRegion.state ?? stateRaw?.toUpperCase() ?? null,
    countryCode: fileRegion.countryCode ?? countryCodeRaw?.toUpperCase() ?? null,
    phone: toNullableString(row.phone)
  }
}

export async function main() {
  const AWS_ACCESS_KEY_ID = await wmill.getVariable("f/secrets/aws_access_key_id")
  const AWS_SECRET_ACCESS_KEY = await wmill.getVariable("f/secrets/aws_secret_access_key")
  const AWS_REGION = await wmill.getVariable("f/secrets/aws_region")
  const S3_BUCKET = await wmill.getVariable("f/secrets/s3_bucket")

  const s3Client = new S3Client({
    accessKeyId: AWS_ACCESS_KEY_ID,
    secretAccessKey: AWS_SECRET_ACCESS_KEY,
    bucket: S3_BUCKET,
    region: AWS_REGION
  })

  const files: { key: string }[] = []
  let listOpts: { prefix: string; maxKeys?: number; startAfter?: string } = {
    prefix: SOURCE_PREFIX
  }
  do {
    const result = await s3Client.list(listOpts)
    files.push(...(result.contents ?? []))
    if (result.isTruncated && result.contents?.length) {
      listOpts = { ...listOpts, startAfter: result.contents.at(-1)!.key }
    } else {
      break
    }
  } while (true)

  const latestByCombo = new Map<string, { date: string; key: string }>()
  for (const file of files) {
    const parsed = parseComboAndDateFromKey(file.key)
    if (!parsed) continue
    const existing = latestByCombo.get(parsed.combo)
    if (!existing || parsed.date > existing.date) {
      latestByCombo.set(parsed.combo, { date: parsed.date, key: file.key })
    }
  }

  let totalRows = 0
  const processedRows: SilverRow[] = []
  const latestFiles = Array.from(latestByCombo.entries()).map(([combo, v]) => ({ combo, key: v.key }))

  let index = 0
  for (const { combo, key } of latestFiles) {
    index += 1
    console.log(`[CLEAN] Reading ${index}/${latestFiles.length}: ${key}`)
    const text = await s3Client.file(key).text()
    let parsed: unknown
    try {
      parsed = JSON.parse(text)
    } catch {
      continue
    }
    if (!Array.isArray(parsed)) continue

    const fileRegion = fileRegionFromCombo(combo)
    for (const row of parsed) {
      if (!row || typeof row !== "object") continue
      totalRows += 1
      const processed = processRawRow(row as RawRow, fileRegion)
      if (processed) processedRows.push(processed)
    }
  }

  console.log(`[CLEAN] Writing ${processedRows.length} cleaned rows to s3://${S3_BUCKET}/${OUTPUT_KEY}`)
  await s3Client.file(OUTPUT_KEY).write(JSON.stringify(processedRows, null, 2))

  return {
    sourceFilesScanned: files.length,
    latestSourceFilesUsed: latestFiles.length,
    totalRows,
    cleanedRows: processedRows.length,
    outputKey: OUTPUT_KEY
  }
}
