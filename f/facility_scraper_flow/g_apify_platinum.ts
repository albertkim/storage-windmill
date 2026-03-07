// package_json: default
import { S3Client } from "bun"
import * as wmill from "windmill-client"

const GOLD_KEY = "facility-scraper/storage-gold/gold.json"
const PLATINUM_KEY = "facility-scraper/storage-platinum/platinum.json"

type GoldRow = Record<string, unknown> & {
  name?: string
  websiteUrlDomain?: string | null
  phone?: string | null
  emails?: string[]
}

type PlatinumRow = {
  name: string
  websiteUrlDomain: string
  emails: string[]
  phones: string[]
  locations: number
  goldEntries: Record<string, unknown>[]
}

const normalizeDomain = (domain: string | null | undefined): string | null => {
  const trimmed = domain?.trim().toLowerCase()
  if (!trimmed) return null
  return trimmed.replace(/^www\./, "")
}

const normalizeEmail = (email: string): string => email.trim().toLowerCase()
const normalizePhone = (phone: string): string => phone.trim()

const unique = (items: string[]): string[] => {
  const seen = new Set<string>()
  for (const item of items) {
    const cleaned = item.trim()
    if (!cleaned) continue
    seen.add(cleaned)
  }
  return [...seen]
}

const stripGoldEmails = (row: GoldRow): Record<string, unknown> => {
  const { emailScrapeEmails: _, hunterIoEmails: __, emails: ___, ...rest } = row
  return rest
}

const loadGold = async (s3Client: S3Client): Promise<GoldRow[]> => {
  const text = await s3Client.file(GOLD_KEY).text()
  const parsed = JSON.parse(text) as unknown
  if (!Array.isArray(parsed)) {
    throw new Error(`${GOLD_KEY} is not an array`)
  }
  return parsed.filter((row): row is GoldRow => Boolean(row && typeof row === "object"))
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

  const goldRows = await loadGold(s3Client)
  const grouped = new Map<string, PlatinumRow>()

  for (const row of goldRows) {
    const domain = normalizeDomain(typeof row.websiteUrlDomain === "string" ? row.websiteUrlDomain : null)
    if (!domain) continue

    const name = typeof row.name === "string" ? row.name : domain
    const rowEmails = Array.isArray(row.emails) ? row.emails.map((email) => normalizeEmail(String(email))) : []
    const rowPhones = typeof row.phone === "string" && row.phone.trim() ? [normalizePhone(row.phone)] : []

    const existing = grouped.get(domain)
    if (!existing) {
      grouped.set(domain, {
        name,
        websiteUrlDomain: domain,
        emails: unique(rowEmails),
        phones: unique(rowPhones),
        locations: 1,
        goldEntries: [stripGoldEmails(row)]
      })
      continue
    }

    existing.goldEntries.push(stripGoldEmails(row))
    existing.emails = unique([...existing.emails, ...rowEmails])
    existing.phones = unique([...existing.phones, ...rowPhones])
    existing.locations = existing.goldEntries.length
  }

  const platinumRows = [...grouped.values()].sort((left, right) => {
    if (left.locations !== right.locations) return right.locations - left.locations
    return left.name.localeCompare(right.name)
  })

  await s3Client.file(PLATINUM_KEY).write(JSON.stringify(platinumRows, null, 2))

  return {
    goldRows: goldRows.length,
    platinumRows: platinumRows.length,
    platinumKey: PLATINUM_KEY
  }
}
