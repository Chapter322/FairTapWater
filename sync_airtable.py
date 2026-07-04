#!/usr/bin/env python3
"""
Sincroniza los registros de una tabla de Airtable con restaurants.json.
Deduplica automáticamente por (Restaurant Name, Address, City) para
evitar el bug de entradas repetidas.

Variables de entorno requeridas:
  AIRTABLE_TOKEN      -> Personal Access Token de Airtable
  AIRTABLE_BASE_ID    -> ID de la base (empieza por "app...")
  AIRTABLE_TABLE_NAME -> Nombre de la tabla (ej. "Restaurants")
"""

import os
import sys
import json
import time
import urllib.request
import urllib.parse

AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE_ID = os.environ["AIRTABLE_BASE_ID"]
AIRTABLE_TABLE_NAME = os.environ["AIRTABLE_TABLE_NAME"]
AIRTABLE_VIEW_ID = os.environ.get("AIRTABLE_VIEW_ID")  # opcional

API_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{urllib.parse.quote(AIRTABLE_TABLE_NAME)}"
OUTPUT_PATH = "restaurants.json"


def fetch_all_records():
    """Descarga todos los registros de Airtable (opcionalmente filtrados
    por una vista concreta), paginando con 'offset'."""
    records = []
    offset = None
    while True:
        params = {"pageSize": 100}
        if AIRTABLE_VIEW_ID:
            params["view"] = AIRTABLE_VIEW_ID
        if offset:
            params["offset"] = offset
        url = API_URL + "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(
            url, headers={"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
        )
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
        time.sleep(0.25)  # respeta el límite de 5 req/s por base
    return records


def build_maps_url(lat, lng):
    if lat is None or lng is None:
        return ""
    return f"https://www.google.com/maps/search/?api=1&query={lat},{lng}"


def normalize(record):
    """Convierte un registro de Airtable al formato de restaurants.json."""
    f = record.get("fields", {})
    lat = f.get("Latitude")
    lng = f.get("Longitude")
    return {
        "Restaurant Name": (f.get("Restaurant Name") or "").strip(),
        "Country": f.get("Country", "Netherlands"),
        "City": (f.get("City") or "").strip(),
        "Address": (f.get("Address") or "").strip(),
        "Latitude": lat,
        "Longitude": lng,
        "MapsURL": f.get("MapsURL") or build_maps_url(lat, lng),
        "Tap Water Policy": f.get("Tap Water Policy", ""),
        "Comments": f.get("Comments", "") or "",
    }


def dedupe(entries):
    """Elimina duplicados exactos por (nombre, dirección, ciudad),
    conservando la primera aparición."""
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
        print(f"Aviso: se descartaron {duplicates_found} duplicado(s).")
    return unique


def main():
    print("Descargando registros de Airtable...")
    records = fetch_all_records()
    print(f"Registros descargados: {len(records)}")

    entries = [normalize(r) for r in records]
    entries = [e for e in entries if e["Restaurant Name"] and e["Latitude"] and e["Longitude"]]
    entries = dedupe(entries)
    entries.sort(key=lambda e: e["Restaurant Name"].lower())

    with open(OUTPUT_PATH, "w", encoding="utf-8") as fh:
        json.dump(entries, fh, ensure_ascii=False, indent=2)
        fh.write("\n")

    print(f"{OUTPUT_PATH} actualizado con {len(entries)} restaurantes únicos.")


if __name__ == "__main__":
    main()
