// package_json: default
import { S3Client } from "bun"
import Papa from "papaparse"
import * as wmill from "windmill-client"

const INPUT_KEY_DEFAULT = "facility-scraper/apify-cleaned/cleaned.json"
const OUTPUT_PREFIX_DEFAULT = "facility-scraper/storage-scraped-emails/"
const OUTPUT_JSON_FILE = "domains_to_emails.json"
const OUTPUT_CSV_FILE = "domains_without_emails.csv"
const HUNTER_IO_PREFIX = "facility-scraper/hunter-io/"
const REQUEST_TIMEOUT_MS = 5000
const PAGE_CONCURRENCY = 10
const SITEMAP_CONCURRENCY = 4
const DOMAIN_CONCURRENCY = 2
const MAX_SITEMAP_PAGES = 20

const emailRegex = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi
const blockedEmailDomains = [
  "domain.com",
  "example.com",
  "sentry.io",
  "wixpress.com",
  "sentry-next.wixpress.com",
  "sentry.wixpress.com",
  "email.com"
]

type CleanedRow = {
  websiteUrlDomain?: string | null
}

type DomainEmailMap = Record<string, string[]>

const normalizeDomain = (value: string | null | undefined): string | null => {
  const raw = value?.trim().toLowerCase()
  if (!raw) return null
  const withoutProtocol = raw.replace(/^https?:\/\//, "")
  const host = withoutProtocol.split("/")[0]?.split("?")[0]?.split("#")[0]?.replace(/\.$/, "") ?? ""
  if (!host) return null
  return host.replace(/^www\./, "")
}

const runWithLimit = async <T, R>(items: T[], limit: number, worker: (item: T) => Promise<R>): Promise<R[]> => {
  if (items.length === 0) return []

  let index = 0
  const results: R[] = []

  const workers = Array.from({ length: Math.min(limit, items.length) }).map(async () => {
    while (true) {
      const currentIndex = index
      index += 1
      if (currentIndex >= items.length) return
      const item = items[currentIndex]
      results.push(await worker(item))
    }
  })

  await Promise.all(workers)
  return results
}

const fetchText = async (url: string, requestTimeoutMs: number): Promise<string | null> => {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), requestTimeoutMs)
  const start = Date.now()

  try {
    console.log(`[EMAIL] Fetching ${url}`)
    const response = await fetch(url, {
      method: "GET",
      signal: controller.signal,
      headers: {
        "user-agent": "Mozilla/5.0 (compatible; WindmillEmailScraper/1.0)"
      }
    })

    if (!response.ok) return null
    return await response.text()
  } catch {
    return null
  } finally {
    clearTimeout(timeout)
    console.log(`[EMAIL] Fetched ${url} in ${Date.now() - start}ms`)
  }
}

const extractLocs = (sitemapText: string): string[] => {
  const locRegex = /<loc>\s*([^<\s]+)\s*<\/loc>/gi
  const locs: string[] = []
  let match: RegExpExecArray | null
  while ((match = locRegex.exec(sitemapText))) {
    locs.push(match[1])
  }
  return locs
}

const getSitemapUrlsFromRobots = async (origin: string, requestTimeoutMs: number): Promise<string[]> => {
  const robotsText = await fetchText(`${origin}/robots.txt`, requestTimeoutMs)
  if (!robotsText) return []

  const lines = robotsText.split(/\r?\n/)
  return lines
    .map((line) => line.trim())
    .filter((line) => line.toLowerCase().startsWith("sitemap:"))
    .map((line) => line.split(":").slice(1).join(":").trim())
    .filter((url) => url.length > 0)
}

const getSitemapPageUrls = async (
  origin: string,
  requestTimeoutMs: number,
  sitemapConcurrency: number,
  maxSitemapPages: number
): Promise<string[]> => {
  const sitemapCandidates = [
    `${origin}/sitemap.xml`,
    `${origin}/sitemap_index.xml`,
    `${origin}/sitemap-index.xml`,
    `${origin}/sitemap/sitemap.xml`
  ]

  const fromRobots = await getSitemapUrlsFromRobots(origin, requestTimeoutMs)
  const sitemapQueue = fromRobots.length > 0 ? fromRobots : sitemapCandidates
  const seenSitemaps = new Set<string>()
  const pageUrls = new Set<string>()
  let reachedPageLimit = false

  while (sitemapQueue.length > 0 && !reachedPageLimit) {
    const batch = sitemapQueue.splice(0, sitemapConcurrency)

    await runWithLimit(batch, sitemapConcurrency, async (sitemapUrl) => {
      if (reachedPageLimit) return
      if (seenSitemaps.has(sitemapUrl)) return
      seenSitemaps.add(sitemapUrl)

      const sitemapText = await fetchText(sitemapUrl, requestTimeoutMs)
      if (!sitemapText) return

      const locs = extractLocs(sitemapText)
      if (locs.length === 0) return

      const isIndex = /<sitemapindex[\s>]/i.test(sitemapText) || locs.every((loc) => loc.endsWith(".xml"))
      if (isIndex) {
        for (const loc of locs) {
          if (!seenSitemaps.has(loc)) sitemapQueue.push(loc)
        }
        return
      }

      for (const loc of locs) {
        if (pageUrls.size >= maxSitemapPages) {
          reachedPageLimit = true
          return
        }
        pageUrls.add(loc)
      }
    })
  }

  return [...pageUrls].slice(0, maxSitemapPages)
}

const normalizeEmail = (email: string): string => email.replace(/[.,;:)\]]+$/g, "").toLowerCase()

const isLikelyAssetEmail = (email: string): boolean => {
  const domain = email.split("@")[1] ?? ""
  const cleanedDomain = domain.split("?")[0].split("#")[0]
  return /\.(png|jpe?g|gif|webp|svg|ico|bmp|tiff?)$/i.test(cleanedDomain)
}

const extractEmails = (html: string): string[] => {
  const matches = html.match(emailRegex)
  if (!matches) return []

  const normalized = matches
    .map(normalizeEmail)
    .filter((item) => item.length > 3)
    .filter((item) => !isLikelyAssetEmail(item))
    .filter((item) => {
      const domain = item.split("@")[1]
      return !domain || !blockedEmailDomains.includes(domain)
    })

  return [...new Set(normalized)]
}

const resolveOrigin = (websiteUrlDomain: string | null): string | null => {
  if (!websiteUrlDomain) return null
  try {
    if (websiteUrlDomain.startsWith("http")) return new URL(websiteUrlDomain).origin
    return new URL(`https://${websiteUrlDomain}`).origin
  } catch {
    return null
  }
}

const collectEmailsForOrigin = async (
  origin: string,
  requestTimeoutMs: number,
  sitemapConcurrency: number,
  pageConcurrency: number,
  maxSitemapPages: number
): Promise<string[]> => {
  const pageUrls = await getSitemapPageUrls(origin, requestTimeoutMs, sitemapConcurrency, maxSitemapPages)
  const urlsToVisit = pageUrls.length > 0 ? pageUrls : [origin]

  const emailSets = await runWithLimit(urlsToVisit, pageConcurrency, async (pageUrl) => {
    const html = await fetchText(pageUrl, requestTimeoutMs)
    if (!html) return []
    return extractEmails(html)
  })

  const combined = new Set<string>()
  for (const emails of emailSets) {
    for (const email of emails) combined.add(email)
  }
  return [...combined]
}

const loadExistingEnriched = async (s3Client: S3Client, outputKey: string): Promise<DomainEmailMap> => {
  try {
    const existingText = await s3Client.file(outputKey).text()
    const parsed = JSON.parse(existingText) as unknown
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {}
    const raw = parsed as DomainEmailMap
    const normalized: DomainEmailMap = {}
    for (const [domain, emails] of Object.entries(raw)) {
      const key = normalizeDomain(domain)
      if (!key) continue
      const existing = normalized[key] ?? []
      normalized[key] = mergeEmails(existing, (emails ?? []).map((email) => normalizeEmail(email)))
    }
    return normalized
  } catch {
    return {}
  }
}

const mergeEmails = (existing: string[], incoming: string[]): string[] => {
  if (existing.length === 0) return incoming
  if (incoming.length === 0) return existing
  const combined = new Set<string>()
  for (const email of existing) combined.add(email)
  for (const email of incoming) combined.add(email)
  return [...combined]
}

const resolveField = (fields: string[], match: string): string | null => {
  const target = match.trim().toLowerCase()
  const found = fields.find((field) => field.trim().toLowerCase() === target)
  return found ?? null
}

const loadHunterDomains = async (s3Client: S3Client): Promise<Set<string>> => {
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
  const domains = new Set<string>()

  for (const csvKey of csvKeys) {
    const text = await s3Client.file(csvKey).text()
    const result = Papa.parse<Record<string, string>>(text, {
      header: true,
      skipEmptyLines: true
    })

    const fields = result.meta.fields ?? []
    const inputDomainField = resolveField(fields, "input domain name")
    const domainField = resolveField(fields, "domain name")

    for (const row of result.data) {
      const domainRaw = (inputDomainField && row[inputDomainField]) || (domainField && row[domainField]) || ""
      const domain = normalizeDomain(domainRaw)
      if (!domain) continue
      domains.add(domain)
    }
  }

  return domains
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

  const outputKey = `${OUTPUT_PREFIX_DEFAULT}${OUTPUT_JSON_FILE}`
  const csvKey = `${OUTPUT_PREFIX_DEFAULT}${OUTPUT_CSV_FILE}`
  const hunterDomains = await loadHunterDomains(s3Client)

  const fileContents = await s3Client.file(INPUT_KEY_DEFAULT).text()
  const parsed = JSON.parse(fileContents) as unknown
  if (!Array.isArray(parsed)) {
    throw new Error(`${INPUT_KEY_DEFAULT} is not an array`)
  }

  const rows = parsed as CleanedRow[]
  const enriched = await loadExistingEnriched(s3Client, outputKey)
  const existingDomainCount = Object.keys(enriched).length
  console.log(
    existingDomainCount > 0
      ? `[EMAIL] Found existing ${OUTPUT_JSON_FILE} with ${existingDomainCount} domains. These will be skipped.`
      : `[EMAIL] No existing ${OUTPUT_JSON_FILE} found. Starting fresh.`
  )

  const domainSet = new Set<string>()
  for (const row of rows) {
    const domain = normalizeDomain(row.websiteUrlDomain)
    if (!domain) continue
    if (domain in enriched) continue
    domainSet.add(domain)
  }

  const domainsToProcess = [...domainSet]
  console.log(`[EMAIL] Processing ${domainsToProcess.length} domains`)
  let completedDomains = 0
  const totalDomains = domainsToProcess.length

  let writeChain = Promise.resolve()
  const enqueueWrite = async (task: () => Promise<void>) => {
    writeChain = writeChain.then(task)
    return writeChain
  }

  await runWithLimit(domainsToProcess, DOMAIN_CONCURRENCY, async (domain) => {
    const origin = resolveOrigin(domain)
    if (!origin) return

    try {
      console.log(`[EMAIL] Collecting emails for ${origin}`)
      const emails = await collectEmailsForOrigin(
        origin,
        REQUEST_TIMEOUT_MS,
        SITEMAP_CONCURRENCY,
        PAGE_CONCURRENCY,
        MAX_SITEMAP_PAGES
      )
      await enqueueWrite(async () => {
        const existing = enriched[domain] ?? []
        enriched[domain] = mergeEmails(existing, emails)
        await s3Client.file(outputKey).write(JSON.stringify(enriched, null, 2))
      })
      completedDomains += 1
      console.log(`[EMAIL] Progress ${completedDomains}/${totalDomains}: ${domain}`)
      console.log(`[EMAIL] Collected ${emails.length} emails for ${origin}`)
    } catch {
      completedDomains += 1
      console.log(`[EMAIL] Progress ${completedDomains}/${totalDomains}: ${domain} (failed)`)
      return
    }
  })

  const domainsWithoutEmails = Object.keys(enriched).filter((domain) => {
    if ((enriched[domain] ?? []).length > 0) return false
    if (hunterDomains.has(domain)) return false
    return true
  })
  const csv = `domain\n${domainsWithoutEmails.map((domain) => `${domain}\n`).join("")}`
  await s3Client.file(csvKey).write(csv)

  return {
    domainsSeen: Object.keys(enriched).length,
    domainsProcessedThisRun: domainsToProcess.length,
    hunterDomainsSeen: hunterDomains.size,
    domainsWithoutEmails: domainsWithoutEmails.length,
    outputKey,
    csvKey
  }
}
