import { ApifyClient } from "apify-client"
import * as wmill from "windmill-client"

export async function main() {
  const token = await wmill.getVariable("f/secrets/apify_api_token")
  console.log(`APIFY token loaded: ${Boolean(token)}`)

  const apifyClient = new ApifyClient({ token })

  const { items } = await apifyClient.actors().list()

  console.log("Actors:", items)

  // Find Google Maps Scraper
  const googleMapsScraper = items.find((actor) => actor.name === "crawler-google-places")
  if (!googleMapsScraper) {
    throw new Error("Google Maps Scraper not found")
  }

  console.log("Google Maps Scraper:", googleMapsScraper)

  // Get runs from actor
  const { items: runs } = await apifyClient.actor(googleMapsScraper.id).runs().list({ limit: 100 })
  console.log("Runs:", runs)

  return { tokenLoaded: Boolean(token) }
}
