// package_json: default
import { ApifyClient } from "apify-client"
import * as wmill from "windmill-client"
import { ALL_COUNTRY_STATE_COMBOS } from "./country_state_combos"

type GoogleMapsScrapeInput = {
  includeWebResults: boolean
  language: string
  locationQuery: string
  maxCrawledPlacesPerSearch: number
  maxImages: number
  maximumLeadsEnrichmentRecords: number
  scrapeContacts: boolean
  scrapeDirectories: boolean
  scrapeImageAuthors: boolean
  scrapePlaceDetailPage: boolean
  scrapeReviewsPersonalData: boolean
  scrapeTableReservationProvider: boolean
  searchStringsArray: string[]
  skipClosedPlaces: boolean
  website: "withWebsite" | "withoutWebsite" | "all"
}

const DEFAULT_DESIRED_SCRAPE_COUNT = 2500

const SCRAPE_COUNT_BY_LOCATION = new Map<string, number>()
for (const entry of ALL_COUNTRY_STATE_COMBOS) {
  SCRAPE_COUNT_BY_LOCATION.set(entry.name.trim().toLowerCase(), entry.desiredScrapeCount)
  SCRAPE_COUNT_BY_LOCATION.set(entry.combo.trim().toLowerCase(), entry.desiredScrapeCount)
}

const resolveDesiredScrapeCount = (stateName: string): number =>
  SCRAPE_COUNT_BY_LOCATION.get(stateName.trim().toLowerCase()) ?? DEFAULT_DESIRED_SCRAPE_COUNT

export async function main(stateName: string) {
  const token = await wmill.getVariable("f/secrets/apify_api_token")
  const apifyClient = new ApifyClient({ token })

  const { items } = await apifyClient.actors().list()
  const actor = items.find((a) => a.name === "crawler-google-places")
  if (!actor) {
    throw new Error("Apify actor crawler-google-places not found")
  }

  const desiredScrapeCount = resolveDesiredScrapeCount(stateName)

  const input: GoogleMapsScrapeInput = {
    includeWebResults: false,
    language: "en",
    locationQuery: stateName,
    maxCrawledPlacesPerSearch: desiredScrapeCount,
    maxImages: 0,
    maximumLeadsEnrichmentRecords: 0,
    scrapeContacts: false,
    scrapeDirectories: false,
    scrapeImageAuthors: false,
    scrapePlaceDetailPage: false,
    scrapeReviewsPersonalData: true,
    scrapeTableReservationProvider: false,
    searchStringsArray: ["self storage"],
    skipClosedPlaces: false,
    website: "withWebsite"
  }

  const run = await apifyClient.actor(actor.id).start(input)

  return {
    actorId: actor.id,
    actorName: actor.name,
    runId: run.id,
    runStatus: run.status,
    input
  }
}
