#!/usr/bin/env python3
"""
HF Radar collector — pulls USWC surface current data from NOAA CoastWatch ERDDAP
and writes it to the local PostgreSQL archive.

Runs hourly via cron. On first run, backfills the last N days.
On subsequent runs, fetches only new data since last ingestion.

Usage:
  python3 collect.py [--dataset DATASET] [--backfill-days N] [--dry-run]
"""

import argparse
import fcntl
import json
import logging
import math
import os
import sys
from datetime import datetime, timedelta, timezone
from urllib.error import URLError
from urllib.request import urlopen, Request

def lock_file(dataset):
    """Per-dataset lock file so one dataset's backfill doesn't block others."""
    return f"/tmp/hfradar-collect-{dataset}.lock"

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ERDDAP_BASE = "https://coastwatch.pfeg.noaa.gov/erddap/griddap"

# Coverage bounds and chunk sizes per dataset.
# Smaller chunks avoid ERDDAP response size limits (~50MB per request is safe).
# Bounds are approximate — ERDDAP will snap to the nearest grid cell.
# Coverage bounds from ERDDAP axis metadata (actual_range).
# Chunk sizes chosen to keep responses under ~50MB.
DATASETS = {
    "uswc_6km": {
        "erddap_id": "ucsdHfrW6",
        "table": "hfr_uswc_6km",
        "chunk_hours": 6,
        "south": 30.25, "north": 49.99, "west": -130.36, "east": -115.81,
    },
    "uswc_2km": {
        "erddap_id": "ucsdHfrW2",
        "table": "hfr_uswc_2km",
        "chunk_hours": 6,
        "south": 30.25, "north": 49.99, "west": -130.36, "east": -115.81,
    },
    "uswc_1km": {
        "erddap_id": "ucsdHfrW1",
        "table": "hfr_uswc_1km",
        "chunk_hours": 3,
        "south": 30.25, "north": 49.99, "west": -130.36, "east": -115.81,
    },
    "uswc_500m": {
        "erddap_id": "ucsdHfrW500",
        "table": "hfr_uswc_500m",
        "chunk_hours": 1,
        "south": 37.455, "north": 38.139, "west": -122.594, "east": -122.046,
    },
}

M_PER_S_TO_KNOTS = 1.94384


def get_db():
    return psycopg2.connect(os.environ["HFR_DATABASE_URL"])


def last_ingested_time(conn, dataset):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(last_time) FROM hfr_ingest_log WHERE dataset = %s",
            (dataset,)
        )
        row = cur.fetchone()
        return row[0] if row and row[0] else None


def fetch_chunk(erddap_id, cfg, t_start, t_end):
    """
    Fetch one time chunk from ERDDAP. Returns list of (time, lat, lon, u, v) tuples.
    Only non-NaN rows included.
    """
    t0 = t_start.strftime('%Y-%m-%dT%H:%M:%SZ')
    t1 = t_end.strftime('%Y-%m-%dT%H:%M:%SZ')
    s, n = cfg["south"], cfg["north"]
    w, e = cfg["west"], cfg["east"]
    query = (
        f"water_u[({t0}):1:({t1})][({s}):1:({n})][({w}):1:({e})]"
        f",water_v[({t0}):1:({t1})][({s}):1:({n})][({w}):1:({e})]"
    )
    url = f"{ERDDAP_BASE}/{erddap_id}.csv?{query}"
    log.debug("Fetching %s", url)

    try:
        with urlopen(url, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
    except URLError as e:
        log.error("ERDDAP fetch failed: %s", e)
        return []

    rows = []
    lines = raw.strip().split("\n")
    for line in lines[2:]:  # skip header + units rows
        parts = line.split(",")
        if len(parts) < 5:
            continue
        try:
            t = datetime.strptime(parts[0], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            lat, lon = float(parts[1]), float(parts[2])
            u_raw, v_raw = float(parts[3]), float(parts[4])
        except ValueError:
            continue
        if math.isnan(u_raw) or math.isnan(v_raw):
            continue
        rows.append((t, lat, lon, u_raw, v_raw))

    return rows


def ingest_chunk(conn, table, dataset, rows, dry_run=False):
    if not rows:
        return 0
    if dry_run:
        log.info("  [dry-run] would insert %d rows into %s", len(rows), table)
        return len(rows)

    with conn.cursor() as cur:
        execute_values(
            cur,
            f"INSERT INTO {table} (time, lat, lon, u, v) VALUES %s ON CONFLICT DO NOTHING",
            rows,
            page_size=5000,
        )
        # Record high-water mark
        max_time = max(r[0] for r in rows)
        cur.execute(
            """
            INSERT INTO hfr_ingest_log (dataset, last_time, rows_added)
            VALUES (%s, %s, %s)
            ON CONFLICT (dataset, last_time) DO UPDATE SET rows_added = EXCLUDED.rows_added
            """,
            (dataset, max_time, len(rows)),
        )
    conn.commit()
    return len(rows)


def run_dataset(conn, dataset_key, backfill_days, dry_run):
    cfg = DATASETS[dataset_key]
    erddap_id = cfg["erddap_id"]
    table = cfg["table"]
    chunk_hours = cfg["chunk_hours"]

    last = last_ingested_time(conn, dataset_key)
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    if last is None:
        start = now - timedelta(days=backfill_days)
        log.info("%s: no prior data, backfilling %d days from %s", dataset_key, backfill_days, start)
    else:
        start = last + timedelta(hours=1)
        log.info("%s: resuming from %s", dataset_key, start)

    if start >= now:
        log.info("%s: already up to date", dataset_key)
        return

    total = 0
    t = start
    while t < now:
        t_end = min(t + timedelta(hours=chunk_hours - 1), now)
        log.info("  fetching %s → %s", t, t_end)
        rows = fetch_chunk(erddap_id, cfg, t, t_end)
        log.info("  got %d valid points", len(rows))
        n = ingest_chunk(conn, table, dataset_key, rows, dry_run=dry_run)
        total += n
        t = t_end + timedelta(hours=1)

    log.info("%s: ingested %d rows total", dataset_key, total)


def check_gaps(conn):
    """
    Check each dataset for staleness. Alert via Discord webhook if any dataset
    has data but hasn't updated in >2 hours (indicating a collection failure,
    not a fresh backfill). Silently skips datasets still in initial backfill
    (last_time older than 7 days) and datasets with no data yet.
    """
    webhook = os.environ.get("HFR_DISCORD_WEBHOOK")
    now = datetime.now(timezone.utc)
    stale = []

    with conn.cursor() as cur:
        for dataset in DATASETS:
            cur.execute(
                "SELECT MAX(last_time) FROM hfr_ingest_log WHERE dataset = %s",
                (dataset,)
            )
            row = cur.fetchone()
            if not row or not row[0]:
                continue
            last = row[0]
            age_hours = (now - last).total_seconds() / 3600
            # Only flag as stale if data is recent enough to not be mid-backfill
            if age_hours > 2 and age_hours < 7 * 24:
                stale.append((dataset, last, age_hours))
                log.warning("GAP: %s last updated %s (%.1fh ago)", dataset, last, age_hours)

    if stale and webhook:
        lines = [f"⚠️ **HF Radar collection gap detected**"]
        for dataset, last, age in stale:
            lines.append(f"• `{dataset}` — last data {last.strftime('%Y-%m-%d %H:%M UTC')} ({age:.1f}h ago)")
        _discord(webhook, "\n".join(lines))
    elif not stale:
        log.info("gap check: all datasets current")


def _discord(webhook_url, message):
    try:
        data = json.dumps({"content": message}).encode()
        req = Request(webhook_url, data=data, headers={"Content-Type": "application/json"})
        urlopen(req, timeout=10)
    except Exception as e:
        log.warning("Discord alert failed: %s", e)


def main():
    parser = argparse.ArgumentParser(description="HF Radar data collector")
    parser.add_argument("--dataset", choices=list(DATASETS) + ["all"], default="all")
    parser.add_argument("--backfill-days", type=int, default=90,
                        help="Days to backfill on first run (default: 90)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch data but don't write to database")
    parser.add_argument("--skip-gap-check", action="store_true",
                        help="Skip staleness check (use during backfill)")
    parser.add_argument("--no-lock", action="store_true",
                        help="Skip lock file (use for dedicated per-dataset runs)")
    args = parser.parse_args()

    conn = get_db()
    targets = list(DATASETS) if args.dataset == "all" else [args.dataset]

    for ds in targets:
        if args.no_lock:
            run_dataset(conn, ds, args.backfill_days, args.dry_run)
            continue

        # Per-dataset lock: backfill for one dataset never blocks another
        lf = lock_file(ds)
        try:
            fh = open(lf, "w")
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            log.info("%s: already running, skipping.", ds)
            continue

        try:
            run_dataset(conn, ds, args.backfill_days, args.dry_run)
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)
            fh.close()

    if not args.dry_run and not args.skip_gap_check and args.dataset == "all":
        check_gaps(conn)
    conn.close()

    conn.close()


if __name__ == "__main__":
    main()
