// package_json: default
import { ApifyClient } from "apify-client"
import * as wmill from "windmill-client"

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

export async function main(stateName: string, searchTerm = "self storage", maxCrawledPlacesPerSearch = 2500) {
  const token = await wmill.getVariable("f/secrets/apify_api_token")
  const apifyClient = new ApifyClient({ token })

  const { items } = await apifyClient.actors().list()
  const actor = items.find((a) => a.name === "crawler-google-places")
  if (!actor) {
    throw new Error("Apify actor crawler-google-places not found")
  }

  const input: GoogleMapsScrapeInput = {
    includeWebResults: false,
    language: "en",
    locationQuery: stateName,
    maxCrawledPlacesPerSearch,
    maxImages: 0,
    maximumLeadsEnrichmentRecords: 0,
    scrapeContacts: false,
    scrapeDirectories: false,
    scrapeImageAuthors: false,
    scrapePlaceDetailPage: false,
    scrapeReviewsPersonalData: true,
    scrapeTableReservationProvider: false,
    searchStringsArray: [searchTerm],
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
