#!/usr/bin/env python3
"""
Syncs Airtable -> restaurants.json, auto-filling Address/City/Country
for new submissions, and notifying about the ones still waiting for a
manually-added Google Maps URL.

Pipeline:
  Form (Youform) -> Zapier -> Airtable "GridView"
      -> [this script] geocodes with Nominatim (OpenStreetMap) and
         fills in Address/City/Country automatically when confident
      -> [you] search Google Maps by hand, paste the real URL into
         the "URL" field (a quick search link is provided for each
         pending item to speed this up)
      -> Airtable computes Latitude/Longitude/MapsURL from that URL
         (unchanged, untouched formulas -> real, high-quality links
         with Google's own place_id when available)
      -> [this script, next run] builds restaurants.json and commits it

Key rules:
- This script NEVER writes to URL, Latitude, Longitude or MapsURL.
  Those stay exactly as they are today: you paste a real Google Maps
  URL, and your existing formulas compute the rest.
- "City" and "Country" are Single Select fields. To avoid duplicate
  options caused by typos, casing, or language differences in the free
  text, we ONLY auto-assign a value if the geocoded result matches
  (case/whitespace-insensitive) an option that ALREADY EXISTS in the
  field. If there's no match, it's left unset and flagged in the
  notification so you can confirm/create it while filling in the URL.

Outputs for the GitHub Actions workflow:
  restaurants.json  -> committed if it changed. Only includes records
                        with a name, coordinates, a confirmed city and
                        a confirmed country (i.e. fully processed ones).
  needs_review.md   -> new submissions still waiting for a URL, with
                        whatever Address/City/Country could be
                        auto-detected plus a quick search link
                        (persistent issue: updated/closed automatically,
                        never duplicated)

Required environment variables:
  AIRTABLE_TOKEN      -> PAT with data.records:read, data.records:write,
                         schema.bases:read
  AIRTABLE_BASE_ID    -> Base ID (starts with "app...")
  AIRTABLE_TABLE_NAME -> Table name or ID
  AIRTABLE_VIEW_ID    -> ID of the "GridView" view (so we see ALL
                         submissions, including unprocessed new ones)
"""

import os
import json
import time
import urllib.request
import urllib.parse
import urllib.error

AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE_ID = os.environ["AIRTABLE_BASE_ID"]
AIRTABLE_TABLE_NAME = os.environ["AIRTABLE_TABLE_NAME"]
AIRTABLE_VIEW_ID = os.environ.get("AIRTABLE_VIEW_ID")

RECORDS_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{urllib.parse.quote(AIRTABLE_TABLE_NAME)}"
META_URL = f"https://api.airtable.com/v0/meta/bases/{AIRTABLE_BASE_ID}/tables"

OUTPUT_PATH = "restaurants.json"
REVIEW_PATH = "needs_review.md"

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {
    "User-Agent": "FairTapWaterSyncBot/1.0 (contact@fairtapwater.com)"
}


# ---------- Airtable: records ----------

def airtable_request(url, method="GET", body=None):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json",
    })
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as err:
        error_body = err.read().decode("utf-8", errors="replace")
        print(f"Airtable API error {err.code} calling {method} {url}")
        print(f"Response body: {error_body}")
        raise


def fetch_all_records():
    records = []
    offset = None
    while True:
        params = {"pageSize": 100}
        if AIRTABLE_VIEW_ID:
            params["view"] = AIRTABLE_VIEW_ID
        if offset:
            params["offset"] = offset
        url = RECORDS_URL + "?" + urllib.parse.urlencode(params)
        data = airtable_request(url)
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
        time.sleep(0.25)
    return records


def chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def push_updates_to_airtable(updates):
    """updates: list of {"id": recordId, "fields": {...}}.
    Only ever contains Address/City/Country -> never URL/Latitude/
    Longitude/MapsURL."""
    for batch in chunked(updates, 10):  # Airtable allows max 10 per PATCH
        airtable_request(RECORDS_URL, method="PATCH", body={"records": batch})
        time.sleep(0.25)


def record_link(record_id):
    base = f"https://airtable.com/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    if AIRTABLE_VIEW_ID:
        base += f"/{AIRTABLE_VIEW_ID}"
    return f"{base}/{record_id}"


# ---------- Airtable: schema (to validate Single Select options) ----------

def fetch_select_options(field_name):
    """Dict {normalized_name: exact_name_as_in_airtable} with the
    existing options of a Single Select field."""
    data = airtable_request(META_URL)
    for table in data.get("tables", []):
        if table.get("id") == AIRTABLE_TABLE_NAME or table.get("name") == AIRTABLE_TABLE_NAME:
            for field in table.get("fields", []):
                if field.get("name") == field_name:
                    choices = field.get("options", {}).get("choices", [])
                    return {c["name"].strip().lower(): c["name"] for c in choices}
    return {}


def match_existing_option(value, options_lookup):
    """Returns the EXACT name (with correct casing) of an already
    existing option if 'value' matches ignoring case/whitespace,
    or None otherwise."""
    if not value:
        return None
    return options_lookup.get(value.strip().lower())


# ---------- Geocoding (Nominatim / OpenStreetMap) ----------

def _nominatim_call(query):
    params = {
        "q": query,
        "format": "jsonv2",
        "addressdetails": 1,
        "limit": 1,
        "accept-language": "en",  # keep country/city names in English, consistently
    }
    url = NOMINATIM_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers=NOMINATIM_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            results = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError):
        results = []
    finally:
        time.sleep(1.1)  # Nominatim usage policy: max 1 request/second
    return results[0] if results else None


def _is_precise_enough(result):
    """True if the result has street-level precision (or better), not
    just "somewhere in this city"."""
    addr = result.get("address", {})
    if addr.get("road"):
        return True
    precise_classes = {"amenity", "shop", "tourism", "office", "leisure"}
    if result.get("class") in precise_classes:
        return True
    return False


def geocode_nominatim(name, raw_location):
    """raw_location is free text as typed by the person on the form:
    it could be just a city, a street + city, or just a street.
    Returns a dict with address/city/country, or None if there's no
    reliable result."""
    candidate_queries = [
        f"{name}, {raw_location}",
        f"{raw_location}",
    ]
    for query in candidate_queries:
        result = _nominatim_call(query)
        if result and _is_precise_enough(result):
            addr = result.get("address", {})
            road = addr.get("road", "")
            house = addr.get("house_number", "")
            address_line = f"{road} {house}".strip() or result.get("display_name", "")
            city = (
                addr.get("city") or addr.get("town") or addr.get("village")
                or addr.get("municipality") or raw_location
            )
            country = addr.get("country", "")
            return {"address": address_line, "city": city, "country": country}
    return None


def quick_search_link(name, hint):
    """A ready-to-click Google Maps search link, so filling in the real
    URL is just: click -> confirm it's the right place -> copy -> paste."""
    query = ", ".join(part for part in (name, hint) if part)
    return f"https://www.google.com/maps/search/{urllib.parse.quote(query)}"


# ---------- Normalization / dedupe ----------

def to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize(record):
    f = record.get("fields", {})
    return {
        "id": record["id"],
        "Restaurant Name": (f.get("Restaurant Name") or "").strip(),
        "CityForm": (f.get("CityForm") or "").strip(),
        "Country": (f.get("Country") or "").strip(),
        "City": (f.get("City") or "").strip(),
        "Address": (f.get("Address") or "").strip(),
        "URL": (f.get("URL") or "").strip(),
        "Latitude": to_float(f.get("Latitude")),
        "Longitude": to_float(f.get("Longitude")),
        "MapsURL": f.get("MapsURL") or "",
        "Tap Water Policy": f.get("Tap Water Policy", ""),
        "Comments": f.get("Comments", "") or "",
    }


def dedupe(entries):
    """Removes exact duplicates by (name, address, city), keeping the
    first occurrence."""
    seen = set()
    unique = []
    duplicates_found = 0
    for e in entries:
        key = (e["Restaurant Name"].lower(), e["Address"].lower(), e["City"].lower())
        if key in seen:
            duplicates_found += 1
            continue
        seen.add(key)
        unique.append(e)
    if duplicates_found:
        print(f"Warning: discarded {duplicates_found} duplicate(s).")
    return unique


# ---------- Main ----------

def main():
    print("Reading table schema (Single Select options)...")
    city_options = fetch_select_options("City")
    country_options = fetch_select_options("Country")
    print(f"Existing cities: {len(city_options)} | Existing countries: {len(country_options)}")

    print("Downloading records from Airtable (GridView)...")
    records = fetch_all_records()
    print(f"Records downloaded: {len(records)}")

    entries = [normalize(r) for r in records]

    airtable_updates = []
    pending = []  # everything still missing a URL -> goes in the notification

    for e in entries:
        if e["URL"]:
            continue  # already has a real URL -> formulas already computed everything, leave alone

        raw_location = e["City"] or e["CityForm"] or e["Address"]
        fields_to_update = {}
        geocode_note = None

        if e["Restaurant Name"] and raw_location:
            result = geocode_nominatim(e["Restaurant Name"], raw_location)
            if result:
                if not e["Address"]:
                    e["Address"] = result["address"]
                    fields_to_update["Address"] = result["address"]

                if not e["City"]:
                    matched_city = match_existing_option(result["city"], city_options)
                    if matched_city:
                        e["City"] = matched_city
                        fields_to_update["City"] = matched_city
                    else:
                        geocode_note = f"detected city not in the list yet: *{result['city']}*"

                if not e["Country"]:
                    matched_country = match_existing_option(result["country"], country_options)
                    if matched_country:
                        e["Country"] = matched_country
                        fields_to_update["Country"] = matched_country
                    elif not geocode_note:
                        geocode_note = f"detected country not in the list yet: *{result['country']}*"
            else:
                geocode_note = "could not auto-detect the location, please look it up manually"

        if fields_to_update:
            airtable_updates.append({"id": e["id"], "fields": fields_to_update})
            print(f"Pre-filled: {e['Restaurant Name']} -> {fields_to_update}")

        pending.append((e, geocode_note))

    if airtable_updates:
        print(f"Writing {len(airtable_updates)} update(s) to Airtable (Address/City/Country only)...")
        push_updates_to_airtable(airtable_updates)

    # Only entries with coordinates, a confirmed city AND a confirmed
    # country make it into the public JSON (these all have a real URL
    # already, since Latitude/Longitude only exist once a URL was pasted)
    publishable = [
        e for e in entries
        if e["Restaurant Name"] and e["Latitude"] and e["Longitude"] and e["City"] and e["Country"]
    ]
    for e in publishable:
        e.pop("id", None)
        e.pop("URL", None)
        e.pop("CityForm", None)

    publishable = dedupe(publishable)
    publishable.sort(key=lambda e: e["Restaurant Name"].lower())

    with open(OUTPUT_PATH, "w", encoding="utf-8") as fh:
        json.dump(publishable, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    print(f"{OUTPUT_PATH} updated with {len(publishable)} unique restaurants.")

    # ---- Pending submissions report (persistent issue -> email) ----
    lines = []
    if pending:
        lines.append(
            "These submissions are still waiting for a Google Maps URL. "
            "Where possible, Address/City/Country were already "
            "auto-detected below for you to double-check while you paste "
            "the URL in Airtable:\n"
        )
        for e, note in pending:
            hint = e["City"] or e["CityForm"] or e["Address"] or "(no location provided)"
            search_link = quick_search_link(e["Restaurant Name"], e["Address"] or hint)
            details = []
            if e["Address"]:
                details.append(f"Address: {e['Address']}")
            if e["City"]:
                details.append(f"City: {e['City']}")
            if e["Country"]:
                details.append(f"Country: {e['Country']}")
            if note:
                details.append(f"⚠️ {note}")
            details_str = f" — {' | '.join(details)}" if details else ""
            lines.append(
                f"- [ ] **{e['Restaurant Name'] or '(no name)'}** — {hint}{details_str} — "
                f"[🔍 Search on Google Maps]({search_link}) — "
                f"[Open in Airtable]({record_link(e['id'])})"
            )

    with open(REVIEW_PATH, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))

    if pending:
        print(f"{len(pending)} submission(s) waiting for a Maps URL (see {REVIEW_PATH}).")


if __name__ == "__main__":
    main()
