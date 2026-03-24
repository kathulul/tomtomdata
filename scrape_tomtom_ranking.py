#!/usr/bin/env python3
"""Fetch TomTom Traffic Index ranking and write center/metro CSVs."""

from __future__ import annotations

import argparse
import csv
import html
import json
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

DEFAULT_URL = "https://www.tomtom.com/traffic-index/ranking/"
DEFAULT_TIMEOUT = 60.0

# Embedded JSON uses short keys; CSV uses stable column names for downstream use.
COLUMNS: dict[str, str] = {
    "key": "city_key",
    "name": "city_name",
    "country": "country_code",
    "countryName": "country_name",
    "continent": "continent",
    "cRank": "rank",
    "c": "congestion_level_pct",
    "cDelta": "congestion_level_delta_pct_pts",
    "v": "travel_time_per_10km_min",
    "d15": "delay_time_per_10km_min",
    "tLostRush": "time_lost_per_year_rush_hours",
    "highwayKmRatio": "highway_km_ratio",
    "population": "population",
}

OUTPUTS: tuple[tuple[str, str], ...] = (
    ("center", "tomtom_traffic_index_center.csv"),
    ("metro", "tomtom_traffic_index_metro.csv"),
)


def unwrap(x: Any) -> Any:
    """Strip Astro serialised [tag, value] wrappers."""
    if isinstance(x, list) and len(x) == 2 and isinstance(x[0], int):
        return unwrap(x[1])
    if isinstance(x, dict):
        return {k: unwrap(v) for k, v in x.items()}
    if isinstance(x, list):
        return [unwrap(i) for i in x]
    return x


def fetch_page(url: str, timeout: float) -> str:
    r = requests.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; TomTomTrafficIndexScraper/1.0)",
            "Accept-Language": "en-US,en;q=0.9",
        },
        timeout=timeout,
    )
    r.raise_for_status()
    return r.text


def load_ranking_props(page_html: str) -> dict[str, Any]:
    for node in BeautifulSoup(page_html, "html.parser").find_all("astro-island"):
        url = node.get("component-url") or ""
        if "/Ranking." not in url:
            continue
        raw = node.get("props")
        if raw:
            return json.loads(html.unescape(raw))
    raise RuntimeError("Ranking data not found (page markup may have changed).")


def load_tables(props: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    data = unwrap(props["rankingData"])
    out: dict[str, list[dict[str, Any]]] = {}
    for key, _fname in OUTPUTS:
        rows = data.get(key)
        if not isinstance(rows, list) or not rows:
            raise RuntimeError(f"Missing or empty rankingData.{key}")
        out[key] = rows
    return out


def project_row(row: dict[str, Any]) -> dict[str, Any]:
    return {COLUMNS[k]: row.get(k) for k in COLUMNS}


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    names = list(COLUMNS.values())
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=names, extrasaction="ignore")
        w.writeheader()
        w.writerows(project_row(r) for r in rows)


def run(url: str, out_dir: Path, timeout: float) -> None:
    props = load_ranking_props(fetch_page(url, timeout))
    if "rankingData" not in props:
        raise RuntimeError("Response missing rankingData.")
    tables = load_tables(props)
    out_dir = out_dir.resolve()
    for key, filename in OUTPUTS:
        p = out_dir / filename
        write_csv(p, tables[key])
        print(f"Wrote {len(tables[key])} rows -> {p}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Export TomTom Traffic Index ranking to CSV.")
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--out-dir", type=Path, default=Path("."))
    ap.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    args = ap.parse_args()
    run(args.url, args.out_dir, args.timeout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
