// package_json: default
import { ApifyClient } from "apify-client"
import { S3Client } from "bun"
import dayjs from "dayjs"
import timezone from "dayjs/plugin/timezone"
import utc from "dayjs/plugin/utc"
import * as wmill from "windmill-client"
import { ALL_COUNTRY_STATE_COMBOS } from "./country_state_combos"

dayjs.extend(utc)
dayjs.extend(timezone)

const S3_PREFIX = "facility-scraper/apify/"
const APIFY_ACTOR_NAME = "crawler-google-places"
const DEFAULT_TIMEZONE = "America/Vancouver"

const buildLocationMap = () => {
  const map = new Map<string, string>()
  for (const item of ALL_COUNTRY_STATE_COMBOS) {
    map.set(item.name.toLowerCase(), item.combo)
    map.set(item.combo.toLowerCase(), item.combo)
  }
  return map
}

const LOCATION_TO_COMBO = buildLocationMap()

function resolveCombo(locationQuery: string): string | null {
  return LOCATION_TO_COMBO.get(locationQuery.trim().toLowerCase()) ?? null
}

function runDateUtc(
  run: {
    startedAt?: Date | string
    createdAt?: Date | string
    finishedAt?: Date | string
  },
  tz: string
): string | null {
  const raw = run.startedAt ?? run.createdAt ?? run.finishedAt
  if (!raw) return null
  const d = raw instanceof Date ? dayjs(raw) : dayjs(raw)
  return d.tz(tz).format("YYYY-MM-DD")
}

async function fetchRunInput(apifyClient: ApifyClient, runId: string): Promise<{ locationQuery?: string } | null> {
  const runClient = apifyClient.run(runId)
  const run = await runClient.get()
  if (!run) return null

  // Some runs expose input directly; otherwise read INPUT from the run key-value store.
  const directInput = (run as { input?: { locationQuery?: string } | null }).input
  if (directInput?.locationQuery) return directInput

  const kvInput = (await runClient.keyValueStore().getRecord("INPUT")) as { value?: { locationQuery?: string } } | null
  return (kvInput?.value as { locationQuery?: string } | undefined) ?? null
}

async function fetchRunResultItems(apifyClient: ApifyClient, runId: string): Promise<unknown[]> {
  const run = await apifyClient.run(runId).get()
  const datasetId = (run as { defaultDatasetId?: string | null } | null)?.defaultDatasetId
  if (!datasetId) return []

  const allItems: unknown[] = []
  const pageSize = 1000
  let offset = 0

  while (true) {
    const page = await apifyClient.dataset(datasetId).listItems({ limit: pageSize, offset })
    const items = page.items ?? []
    if (items.length === 0) break
    allItems.push(...items)
    if (items.length < pageSize) break
    offset += pageSize
  }

  return allItems
}

export async function main(runLimit = 100) {
  const apifyToken = await wmill.getVariable("f/secrets/apify_api_token")
  const AWS_ACCESS_KEY_ID = await wmill.getVariable("f/secrets/aws_access_key_id")
  const AWS_SECRET_ACCESS_KEY = await wmill.getVariable("f/secrets/aws_secret_access_key")
  const AWS_REGION = await wmill.getVariable("f/secrets/aws_region")
  const S3_BUCKET = await wmill.getVariable("f/secrets/s3_bucket")

  const apifyClient = new ApifyClient({ token: apifyToken })
  const { items: actors } = await apifyClient.actors().list()
  const actor = actors.find((a) => a.name === APIFY_ACTOR_NAME)
  if (!actor) throw new Error(`Apify actor not found: ${APIFY_ACTOR_NAME}`)

  const { items: runs } = await apifyClient.actor(actor.id).runs().list({ limit: runLimit, desc: true })

  const s3Client = new S3Client({
    accessKeyId: AWS_ACCESS_KEY_ID,
    secretAccessKey: AWS_SECRET_ACCESS_KEY,
    bucket: S3_BUCKET,
    region: AWS_REGION
  })

  const existingKeys = new Set<string>()
  let listOpts: { prefix: string; maxKeys?: number; startAfter?: string } = {
    prefix: S3_PREFIX
  }
  do {
    const result = await s3Client.list(listOpts)
    for (const f of result.contents ?? []) existingKeys.add(f.key)
    if (result.isTruncated && result.contents?.length) {
      listOpts = { ...listOpts, startAfter: result.contents.at(-1)!.key }
    } else {
      break
    }
  } while (true)

  const missingUploads: Array<{
    runId: string
    locationQuery: string
    combo: string
    date: string
    expectedKey: string
  }> = []
  const unresolvedRuns: Array<{ runId: string; locationQuery: string | null }> = []
  const uploadedKeys: string[] = []
  let stoppedOnExistingUpload = false
  let runsProcessed = 0

  for (const run of runs) {
    runsProcessed += 1
    const date = runDateUtc(run, DEFAULT_TIMEZONE)
    if (!date || !run.id) continue

    const input = await fetchRunInput(apifyClient, run.id)
    const locationQuery = input?.locationQuery?.trim() ?? null
    if (!locationQuery) {
      unresolvedRuns.push({ runId: run.id, locationQuery: null })
      continue
    }

    const combo = resolveCombo(locationQuery)
    if (!combo) {
      unresolvedRuns.push({ runId: run.id, locationQuery })
      continue
    }

    const expectedKey = `${S3_PREFIX}${combo}-${date}.json`
    if (existingKeys.has(expectedKey)) {
      stoppedOnExistingUpload = true
      break
    } else {
      console.log(`[UPLOAD] Missing in S3 for run=${run.id} combo=${combo} date=${date} expected=${expectedKey}`)
      const allItems = await fetchRunResultItems(apifyClient, run.id)
      const payload = JSON.stringify(allItems, null, 2)
      await s3Client.file(expectedKey).write(payload)
      existingKeys.add(expectedKey)
      uploadedKeys.push(expectedKey)
      console.log(`[UPLOAD] Wrote ${allItems.length} items to s3://${S3_BUCKET}/${expectedKey}`)

      missingUploads.push({ runId: run.id, locationQuery, combo, date, expectedKey })
    }
  }

  return {
    runsProcessed,
    stoppedOnExistingUpload,
    uploadedCount: uploadedKeys.length,
    unresolvedRunCount: unresolvedRuns.length
  }
}
