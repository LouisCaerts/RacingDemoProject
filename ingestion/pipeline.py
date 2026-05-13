"""
F1 Data Ingestion Pipeline
Source : Jolpica F1 API (Ergast-compatible)
Dest   : Snowflake (via dlt)

Loads the following raw tables into Snowflake:
  - raw_seasons
  - raw_circuits
  - raw_races
  - raw_constructors
  - raw_drivers
  - raw_results
  - raw_sprint
  - raw_qualifying
  - raw_pitstops
  - raw_laps
  - raw_driverstandings
  - raw_constructorstandings
  - raw_status
"""

import dlt
from dlt.sources.helpers import requests

BASE_URL = "https://api.jolpi.ca/ergast/f1"
LIMIT    = 1000  # max per page; Jolpica allows up to 1000

# Set to a list of ints to restrict ingestion, e.g. [2022, 2023, 2024].
# None fetches the full history.
SEASONS: list[int] | None = [2023, 2024, 2025]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def paginate(url: str) -> list[dict]:
    """Fetch all pages from a Jolpica endpoint and return combined records."""
    results = []
    offset  = 0

    while True:
        response = requests.get(url, params={"limit": LIMIT, "offset": offset})
        response.raise_for_status()

        data  = response.json()["MRData"]
        total = int(data["total"])
        print(f"  url={url} offset={offset} total={total} fetched={len(results)}")

        # Each endpoint nests its records under a different key.
        # The second key in MRData (after xmlns/series/url/limit/offset/total)
        # is always the table key (e.g. "DriverTable", "RaceTable" …).
        table_key  = [k for k in data if k.endswith("Table")][0]
        record_key = list(data[table_key].keys())[-1]   # e.g. "Drivers"
        results.extend(data[table_key][record_key])

        offset += LIMIT
        if offset >= total:
            break

    return results


def get_race_keys(seasons: list[int] | None = None) -> list[tuple[int, int]]:
    """Return (season, round) pairs for every race in scope."""
    if seasons:
        all_races = []
        for s in seasons:
            all_races.extend(paginate(f"{BASE_URL}/{s}/races.json"))
    else:
        all_races = paginate(f"{BASE_URL}/races.json")
    return [(int(r["season"]), int(r["round"])) for r in all_races]


# ---------------------------------------------------------------------------
# dlt resources
# ---------------------------------------------------------------------------

@dlt.resource(name="raw_seasons", write_disposition="replace")
def seasons():
    yield paginate(f"{BASE_URL}/seasons.json")

@dlt.resource(name="raw_circuits", write_disposition="replace")
def circuits():
    yield paginate(f"{BASE_URL}/circuits.json")

@dlt.resource(name="raw_races", write_disposition="replace")
def races(filter_seasons: list[int] | None = None):
    if filter_seasons:
        for s in filter_seasons:
            yield paginate(f"{BASE_URL}/{s}/races.json")
    else:
        yield paginate(f"{BASE_URL}/races.json")

@dlt.resource(name="raw_constructors", write_disposition="replace")
def constructors():
    yield paginate(f"{BASE_URL}/constructors.json")

@dlt.resource(name="raw_drivers", write_disposition="replace")
def drivers():
    yield paginate(f"{BASE_URL}/drivers.json")

@dlt.resource(name="raw_results", write_disposition="replace")
def results(filter_seasons: list[int] | None = None):
    if filter_seasons:
        for s in filter_seasons:
            yield paginate(f"{BASE_URL}/{s}/results.json")
    else:
        yield paginate(f"{BASE_URL}/results.json")

@dlt.resource(name="raw_sprint", write_disposition="replace")
def sprint(filter_seasons: list[int] | None = None):
    if filter_seasons:
        for s in filter_seasons:
            yield paginate(f"{BASE_URL}/{s}/sprint.json")
    else:
        yield paginate(f"{BASE_URL}/sprint.json")

@dlt.resource(name="raw_qualifying", write_disposition="replace")
def qualifying(filter_seasons: list[int] | None = None):
    if filter_seasons:
        for s in filter_seasons:
            yield paginate(f"{BASE_URL}/{s}/qualifying.json")
    else:
        yield paginate(f"{BASE_URL}/qualifying.json")

@dlt.resource(name="raw_pitstops", write_disposition="replace")
def pitstops(filter_seasons: list[int] | None = None):
    for season, round_num in get_race_keys(filter_seasons):
        records = paginate(f"{BASE_URL}/{season}/{round_num}/pitstops.json")
        for r in records:
            r["_season"] = season
            r["_round"] = round_num
        yield records

@dlt.resource(name="raw_laps", write_disposition="replace")
def laps(filter_seasons: list[int] | None = None):
    for season, round_num in get_race_keys(filter_seasons):
        records = paginate(f"{BASE_URL}/{season}/{round_num}/laps.json")
        for r in records:
            r["_season"] = season
            r["_round"] = round_num
        yield records

@dlt.resource(name="raw_driverstandings", write_disposition="replace")
def driverstandings(filter_seasons: list[int] | None = None):
    for season, round_num in get_race_keys(filter_seasons):
        records = paginate(f"{BASE_URL}/{season}/{round_num}/driverstandings.json")
        for r in records:
            r["_season"] = season
            r["_round"] = round_num
        yield records

@dlt.resource(name="raw_constructorstandings", write_disposition="replace")
def constructorstandings(filter_seasons: list[int] | None = None):
    for season, round_num in get_race_keys(filter_seasons):
        records = paginate(f"{BASE_URL}/{season}/{round_num}/constructorstandings.json")
        for r in records:
            r["_season"] = season
            r["_round"] = round_num
        yield records

@dlt.resource(name="raw_status", write_disposition="replace")
def status():
    yield paginate(f"{BASE_URL}/status.json")


# ---------------------------------------------------------------------------
# Source + pipeline
# ---------------------------------------------------------------------------

@dlt.source
def f1_source(filter_seasons: list[int] | None = None):
    filter_seasons = filter_seasons or SEASONS
    return [
        seasons(),
        circuits(),
        races(filter_seasons),
        constructors(),
        drivers(),
        results(filter_seasons),
        sprint(filter_seasons),
        qualifying(filter_seasons),
        pitstops(filter_seasons),
        laps(filter_seasons),
        driverstandings(filter_seasons),
        constructorstandings(filter_seasons),
        status(),
    ]


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="f1_pipeline",
        destination="snowflake",
        dataset_name="f1_raw",
    )

    load_info = pipeline.run(f1_source())
    print(load_info)