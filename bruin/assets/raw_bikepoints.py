""" @bruin
name: san_cycles_raw.bikepoints
type: python
image: python:3.12
connection: san_cycles

materialization:
    type: table
    strategy: create+replace

description: |
  Sources all active Santander Cycles docking stations from the TfL
  BikePoint API and enriches each with its London borough via reverse
  geocoding through the postcodes.io API (no auth required for either).

  TerminalName (TfL) maps to station_id in journey data, though station IDs
  have changed over the years so some historical IDs will not match.

@bruin """

import math
import requests
import pandas as pd

TFL_URL        = "https://api.tfl.gov.uk/BikePoint"
POSTCODES_URL  = "https://api.postcodes.io/postcodes"
BATCH_SIZE     = 100


def _fetch_bikepoints() -> list[dict]:
    resp = requests.get(TFL_URL, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _parse_bikepoints(places: list[dict]) -> pd.DataFrame:
    records = []
    for place in places:
        props = {p["key"]: p["value"] for p in place.get("additionalProperties", [])}
        terminal = props.get("TerminalName")
        if not terminal:
            continue
        try:
            station_id = int(terminal)
        except (ValueError, TypeError):
            continue
        records.append({
            "station_id":   station_id,
            "station_name": place.get("commonName"),
            "lat":          place.get("lat"),
            "lon":          place.get("lon"),
        })
    return pd.DataFrame(records)


def _reverse_geocode_boroughs(df: pd.DataFrame) -> list[str | None]:
    boroughs = [None] * len(df)
    n_batches = math.ceil(len(df) / BATCH_SIZE)

    for i in range(n_batches):
        batch_df = df.iloc[i * BATCH_SIZE : (i + 1) * BATCH_SIZE]
        geolocations = [
            {"latitude": row["lat"], "longitude": row["lon"], "limit": 1}
            for _, row in batch_df.iterrows()
        ]
        try:
            resp = requests.post(POSTCODES_URL, json={"geolocations": geolocations}, timeout=30)
            resp.raise_for_status()
            results = resp.json().get("result", [])
            for j, result in enumerate(results):
                idx = i * BATCH_SIZE + j
                nearest = (result or {}).get("result")
                if nearest:
                    boroughs[idx] = nearest[0].get("admin_district")
        except Exception as e:
            print(f"Geocoding batch {i} failed: {e}")

    return boroughs


def materialize():
    print("Fetching stations from TfL BikePoint API...")
    places = _fetch_bikepoints()
    print(f"  {len(places)} stations returned")

    df = _parse_bikepoints(places)
    print(f"  {len(df)} stations with valid TerminalName")

    print("Reverse geocoding boroughs via postcodes.io...")
    df["borough"] = _reverse_geocode_boroughs(df)
    matched = df["borough"].notna().sum()
    print(f"  Borough matched: {matched}/{len(df)}")

    return df
