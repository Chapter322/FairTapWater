#!/usr/bin/env python3
"""
Syncs Airtable <-> restaurants.json, automating the full pipeline:

  Form (Youform) -> Zapier -> Airtable "GridView"
      -> [this script] geocodes, fills in Country/City/Address/URL
      -> Airtable computes Latitude/Longitude/MapsURL (formulas from URL)
      -> restaurants.json -> commit to GitHub

Key rules:
- We NEVER write to Latitude/Longitude/MapsURL (they are formula fields ->
  the API would reject the write). Instead we write to "URL" using a
  "!3d..!4d.." pattern that those formulas already understand.
- "City" and "Country" are Single Select fields. To avoid duplicate
  options caused by typos, casing, or language differences in the free
  text, we ONLY auto-assign a value if the geocoded result matches
  (case/whitespace-insensitive) an option that ALREADY EXISTS in the
  field. If there's no match, we leave it unset and flag it for manual
  review (even though Address/URL do get filled in).

Outputs for the GitHub Actions workflow:
  restaurants.json  -> committed if it changed
  needs_review.md   -> pending issues (persistent issue: updated/closed
                        automatically, never duplicated)
  new_entries.md    -> this run's new additions, for an accuracy review
                        (a fresh issue is created every time there's content)

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
NEW_ENTRIES_PATH = "new_entries.md"

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
    Only EDITABLE fields (never Latitude/Longitude/MapsURL)."""
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
    # A shop/amenity-class result is usually precise even if Nominatim
    # didn't fill in 'road' for that specific POI.
    precise_classes = {"amenity", "shop", "tourism", "office", "leisure"}
    if result.get("class") in precise_classes:
        return True
    return False


def geocode_nominatim(name, raw_location):
    """raw_location is free text as typed by the person on the form:
    it could be just a city, a street + city, or just a street.
    No longer restricted to countrycodes=nl, since Country is now also
    derived from the result (in case entries outside NL show up someday).
    Returns a dict with lat/lon/address/city/country, or None if there's
    no reliable result."""
    candidate_queries = [
        f"{name}, {raw_location}",   # business name + whatever they typed
        f"{raw_location}",            # in case raw_location is already "Street 5, City"
    ]

    for query in candidate_queries:
        result = _nominatim_call(query)
        if result and _is_precise_enough(result):
            lat = float(result["lat"])
            lon = float(result["lon"])
            addr = result.get("address", {})
            road = addr.get("road", "")
            house = addr.get("house_number", "")
            address_line = f"{road} {house}".strip() or result.get("display_name", "")
            city = (
                addr.get("city") or addr.get("town") or addr.get("village")
                or addr.get("municipality") or raw_location
            )
            country = addr.get("country", "")
            return {
                "lat": lat, "lon": lon, "address": address_line,
                "city": city, "country": country,
            }

    return None


def build_synthetic_google_maps_url(name, lat, lon):
    """Builds a Google Maps URL using the '!3d..!4d..' pattern that this
    base's Airtable formulas already recognize (MapsURL formula's third
    branch: contains '/place/' and '!3d' but not '/place/ChIJ' nor '!1s')."""
    safe_name = urllib.parse.quote(name.strip() or "restaurant")
    return f"https://www.google.com/maps/place/{safe_name}/@{lat},{lon},17z!3d{lat}!4d{lon}"


def clean_maps_url(lat, lon):
    """Exact equivalent of what Airtable's MapsURL formula produces for a
    URL built with build_synthetic_google_maps_url()."""
    return f"https://www.google.com/maps/@{lat},{lon},17z"


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
    needs_review = []          # no coordinates at all
    needs_city_review = []     # coordinates OK, new city not confirmed yet
    needs_country_review = []  # coordinates OK, new country not confirmed yet
    newly_processed = []       # everything geocoded THIS run (for an accuracy review)

    for e in entries:
        if e["URL"]:
            continue  # already processed before, formulas already computed everything

        raw_location = e["City"] or e["CityForm"] or e["Address"]
        if not e["Restaurant Name"] or not raw_location:
            needs_review.append(e)
            continue

        result = geocode_nominatim(e["Restaurant Name"], raw_location)
        if result is None:
            needs_review.append(e)
            continue

        lat, lon = result["lat"], result["lon"]
        synthetic_url = build_synthetic_google_maps_url(e["Restaurant Name"], lat, lon)
        fields_to_update = {"URL": synthetic_url}

        if not e["Address"]:
            e["Address"] = result["address"]
            fields_to_update["Address"] = result["address"]

        matched_city = match_existing_option(result["city"], city_options)
        if matched_city:
            e["City"] = matched_city
            fields_to_update["City"] = matched_city
        else:
            needs_city_review.append((e, result["city"]))

        matched_country = match_existing_option(result["country"], country_options)
        if matched_country:
            e["Country"] = matched_country
            fields_to_update["Country"] = matched_country
        else:
            needs_country_review.append((e, result["country"]))

        # Mirror in memory what Airtable's formulas will compute from this URL
        e["URL"] = synthetic_url
        e["Latitude"] = lat
        e["Longitude"] = lon
        e["MapsURL"] = clean_maps_url(lat, lon)

        airtable_updates.append({"id": e["id"], "fields": fields_to_update})
        newly_processed.append({
            "entry": e,
            "geocoded_city": result["city"],
            "geocoded_country": result["country"],
            "matched_city": bool(matched_city),
            "matched_country": bool(matched_country),
        })
        print(f"Geocoded: {e['Restaurant Name']} -> {e['Address']}, "
              f"{matched_city or '(' + result['city'] + ' pending)'}, "
              f"{matched_country or '(' + result['country'] + ' pending)'} "
              f"({lat}, {lon})")

    if airtable_updates:
        print(f"Writing {len(airtable_updates)} update(s) to Airtable...")
        push_updates_to_airtable(airtable_updates)

    # Only entries with coordinates, a confirmed city AND a confirmed
    # country make it into the public JSON
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

    # ---- Pending items report (persistent issue) ----
    lines = []
    if needs_review:
        lines.append("### Could not be geocoded\n")
        lines.append(
            "Could not be located automatically (missing info, or not "
            "found on OpenStreetMap). Look them up on Google Maps and "
            "paste the URL manually:\n"
        )
        for e in needs_review:
            hint = e["City"] or e["CityForm"] or e["Address"] or "(no location provided)"
            lines.append(
                f"- [ ] **{e['Restaurant Name'] or '(no name)'}** — {hint} — "
                f"[Open in Airtable]({record_link(e['id'])})"
            )
        lines.append("")

    if needs_city_review:
        lines.append("### Coordinates ready, city needs confirming/creating\n")
        for e, geocoded_city in needs_city_review:
            lines.append(
                f"- [ ] **{e['Restaurant Name']}** — detected city: *{geocoded_city}* — "
                f"[Open in Airtable]({record_link(e['id'])})"
            )
        lines.append("")

    if needs_country_review:
        lines.append("### Coordinates ready, country needs confirming/creating\n")
        for e, geocoded_country in needs_country_review:
            lines.append(
                f"- [ ] **{e['Restaurant Name']}** — detected country: *{geocoded_country}* — "
                f"[Open in Airtable]({record_link(e['id'])})"
            )
        lines.append("")

    with open(REVIEW_PATH, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))

    total_pending = len(needs_review) + len(needs_city_review) + len(needs_country_review)
    if total_pending:
        print(f"{total_pending} item(s) pending manual review (see {REVIEW_PATH}).")

    # ---- New entries report for this run (a fresh issue every time) ----
    new_lines = []
    if newly_processed:
        new_lines.append(
            "These restaurants were geocoded automatically in this run. "
            "Please double-check the location is correct (in case "
            "Nominatim matched the wrong business with a similar name):\n"
        )
        for item in newly_processed:
            e = item["entry"]
            maps_link = clean_maps_url(e["Latitude"], e["Longitude"])
            city_note = e["City"] if item["matched_city"] else f"⚠️ {item['geocoded_city']} (pending)"
            country_note = e["Country"] if item["matched_country"] else f"⚠️ {item['geocoded_country']} (pending)"
            new_lines.append(
                f"- **{e['Restaurant Name']}** — {e['Address']}, {city_note}, {country_note} — "
                f"[View on Google Maps]({maps_link}) — [Open in Airtable]({record_link(e['id'])})"
            )

    with open(NEW_ENTRIES_PATH, "w", encoding="utf-8") as fh:
        fh.write("\n".join(new_lines))


if __name__ == "__main__":
    main()
