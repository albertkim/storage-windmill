// package_json: default
import { S3Client } from "bun"
import Papa from "papaparse"
import * as wmill from "windmill-client"

const CLEANED_KEY = "facility-scraper/apify-cleaned/cleaned.json"
const EMAIL_SCRAPE_KEY = "facility-scraper/storage-scraped-emails/domains_to_emails.json"
const HUNTER_IO_PREFIX = "facility-scraper/hunter-io/"
const GOLD_KEY = "facility-scraper/storage-gold/gold.json"

type SilverRow = Record<string, unknown> & {
  websiteUrlDomain?: string | null
}

type GoldRow = SilverRow & {
  emailScrapeEmails: string[]
  hunterIoEmails: string[]
  emails: string[]
}

const normalizeDomain = (value: string | null | undefined): string | null => {
  const raw = value?.trim().toLowerCase()
  if (!raw) return null
  const withoutProtocol = raw.replace(/^https?:\/\//, "")
  const host = withoutProtocol.split("/")[0]?.split("?")[0]?.split("#")[0]?.replace(/\.$/, "") ?? ""
  if (!host) return null
  return host.replace(/^www\./, "")
}

const normalizeEmail = (email: string): string => email.trim().toLowerCase()

const domainMatches = (source: string, target: string): boolean => {
  if (source === target) return true
  return source.endsWith(`.${target}`)
}

const unique = (items: string[]): string[] => {
  const seen = new Set<string>()
  for (const item of items) {
    const cleaned = item.trim()
    if (!cleaned) continue
    seen.add(cleaned)
  }
  return [...seen]
}

const resolveField = (fields: string[], match: string): string | null => {
  const target = match.trim().toLowerCase()
  const found = fields.find((field) => field.trim().toLowerCase() === target)
  return found ?? null
}

const collectHunterEmails = async (s3Client: S3Client): Promise<Record<string, string[]>> => {
  const files: { key: string }[] = []
  let listOpts: { prefix: string; maxKeys?: number; startAfter?: string } = {
    prefix: HUNTER_IO_PREFIX
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

  const csvKeys = files.map((f) => f.key).filter((key) => key.toLowerCase().endsWith(".csv"))
  const domainToEmails: Record<string, string[]> = {}

  for (const csvKey of csvKeys) {
    const text = await s3Client.file(csvKey).text()
    const result = Papa.parse<Record<string, string>>(text, {
      header: true,
      skipEmptyLines: true
    })

    const fields = result.meta.fields ?? []
    const inputDomainField = resolveField(fields, "input domain name")
    const domainField = resolveField(fields, "domain name")
    const emailField = resolveField(fields, "email address")
    if (!emailField) continue

    for (const row of result.data) {
      const domainRaw = (inputDomainField && row[inputDomainField]) || (domainField && row[domainField]) || ""
      const domain = normalizeDomain(domainRaw)
      if (!domain) continue

      const emailRaw = (emailField && row[emailField]) || ""
      const emails = emailRaw
        .split(/[;,]+/g)
        .map((entry) => normalizeEmail(entry))
        .filter((entry) => entry.length > 0)
      if (emails.length === 0) continue

      const existing = domainToEmails[domain] ?? []
      domainToEmails[domain] = unique([...existing, ...emails])
    }
  }

  return domainToEmails
}

const readEmailScrape = async (s3Client: S3Client): Promise<Record<string, string[]>> => {
  const text = await s3Client.file(EMAIL_SCRAPE_KEY).text()
  const parsed = JSON.parse(text) as unknown
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(`${EMAIL_SCRAPE_KEY} is not an object mapping domain -> emails[]`)
  }

  const raw = parsed as Record<string, string[]>
  const normalized: Record<string, string[]> = {}
  for (const [domain, emails] of Object.entries(raw)) {
    const normalizedDomain = normalizeDomain(domain)
    if (!normalizedDomain) continue
    normalized[normalizedDomain] = unique((emails ?? []).map((email) => normalizeEmail(email)))
  }
  return normalized
}

const resolveHunterEmails = (domain: string | null, hunterEmails: Record<string, string[]>): string[] => {
  if (!domain) return []
  const direct = hunterEmails[domain]
  if (direct) return direct

  const matches: string[] = []
  for (const [hunterDomain, emails] of Object.entries(hunterEmails)) {
    if (!domainMatches(domain, hunterDomain)) continue
    matches.push(...emails)
  }
  return unique(matches)
}

const loadCleaned = async (s3Client: S3Client): Promise<SilverRow[]> => {
  const text = await s3Client.file(CLEANED_KEY).text()
  const parsed = JSON.parse(text) as unknown
  if (!Array.isArray(parsed)) {
    throw new Error(`${CLEANED_KEY} is not an array`)
  }
  return parsed.filter((row): row is SilverRow => Boolean(row && typeof row === "object"))
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

  const [cleanedRows, emailScrape, hunterEmails] = await Promise.all([
    loadCleaned(s3Client),
    readEmailScrape(s3Client),
    collectHunterEmails(s3Client)
  ])

  const goldRows: GoldRow[] = cleanedRows.map((row) => {
    const domain = normalizeDomain(typeof row.websiteUrlDomain === "string" ? row.websiteUrlDomain : null)
    const emailScrapeEmails = domain ? (emailScrape[domain] ?? []) : []
    const hunterIoEmails = resolveHunterEmails(domain, hunterEmails)
    const emails = unique([...emailScrapeEmails, ...hunterIoEmails])

    return {
      ...row,
      emailScrapeEmails,
      hunterIoEmails,
      emails
    }
  })

  await s3Client.file(GOLD_KEY).write(JSON.stringify(goldRows, null, 2))

  return {
    cleanedRows: cleanedRows.length,
    emailScrapeDomains: Object.keys(emailScrape).length,
    hunterIoDomains: Object.keys(hunterEmails).length,
    goldRows: goldRows.length,
    goldKey: GOLD_KEY
  }
}
