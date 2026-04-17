"""
HF Radar API — FastAPI application
Serves surface current data from the local PostgreSQL archive.

Endpoints:
  GET /currents/point   — nearest valid reading at (lat, lon, time)
  GET /currents/series  — time series at (lat, lon) over a time range
  GET /currents/area    — all readings in a bounding box at a given time
  GET /status           — data freshness and coverage summary
  GET /health           — liveness check
"""

import math
import os
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)

app = FastAPI(
    title="HF Radar Surface Current API",
    description="Historical US West Coast HF radar surface current data, archived beyond the standard 90-day window.",
    version="0.1.0",
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Resolution tables ordered finest → coarsest for "best available" queries
RESOLUTION_TABLES = [
    ("500m", "hfr_uswc_500m"),
    ("1km",  "hfr_uswc_1km"),
    ("2km",  "hfr_uswc_2km"),
    ("6km",  "hfr_uswc_6km"),
]

MAX_SERIES_HOURS = 30 * 24   # 30 days
MAX_AREA_POINTS = 10_000


def get_db():
    return psycopg2.connect(
        os.environ["HFR_DATABASE_URL"],
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def uv_to_speed_dir(u, v):
    speed = math.sqrt(u ** 2 + v ** 2)
    direction = math.degrees(math.atan2(u, v)) % 360
    return round(speed, 4), round(direction, 1)


def row_to_dict(row, resolution):
    u, v = float(row["u"]), float(row["v"])
    speed, direction = uv_to_speed_dir(u, v)
    return {
        "time": row["time"].isoformat(),
        "lat": round(float(row["lat"]), 6),
        "lon": round(float(row["lon"]), 6),
        "u_ms": round(u, 4),
        "v_ms": round(v, 4),
        "speed_ms": speed,
        "direction_deg": direction,
        "resolution": resolution,
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/status")
@limiter.limit("60/minute")
def status(request: Request):
    conn = get_db()
    results = {}
    try:
        with conn.cursor() as cur:
            for res, table in RESOLUTION_TABLES:
                cur.execute(
                    f"SELECT MIN(time), MAX(time), COUNT(*) FROM {table}"
                )
                row = cur.fetchone()
                results[res] = {
                    "earliest": row["min"].isoformat() if row["min"] else None,
                    "latest":   row["max"].isoformat() if row["max"] else None,
                    "total_rows": row["count"],
                }
    finally:
        conn.close()
    return results


@app.get("/currents/point")
@limiter.limit("120/minute")
def currents_point(
    request: Request,
    lat: float = Query(..., description="Latitude (decimal degrees)"),
    lon: float = Query(..., description="Longitude (decimal degrees, negative=west)"),
    time: str  = Query(..., description="UTC time (ISO 8601, e.g. 2026-03-10T14:00:00Z)"),
    resolution: Optional[str] = Query(None, description="Force resolution: 500m, 1km, 2km, 6km (default: finest available)"),
):
    """Return the nearest valid surface current reading to (lat, lon) at the given time."""
    try:
        t = datetime.fromisoformat(time.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(400, "Invalid time format. Use ISO 8601, e.g. 2026-03-10T14:00:00Z")

    tables = [(r, tbl) for r, tbl in RESOLUTION_TABLES if resolution is None or r == resolution]
    if not tables:
        raise HTTPException(400, f"Unknown resolution '{resolution}'. Use: 500m, 1km, 2km, 6km")

    conn = get_db()
    try:
        with conn.cursor() as cur:
            for res, table in tables:
                # KNN nearest neighbor within ±1 hour of requested time
                cur.execute(
                    f"""
                    SELECT lat, lon, u, v, time
                    FROM {table}
                    WHERE time BETWEEN %s AND %s
                    ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                    LIMIT 1
                    """,
                    (t - timedelta(hours=1),
                     t + timedelta(hours=1),
                     lon, lat),
                )
                row = cur.fetchone()
                if row:
                    return row_to_dict(row, res)
    finally:
        conn.close()

    raise HTTPException(404, "No data found at this location and time.")


@app.get("/currents/series")
@limiter.limit("20/minute")
def currents_series(
    request: Request,
    lat:   float = Query(...),
    lon:   float = Query(...),
    start: str   = Query(..., description="Start time (ISO 8601)"),
    end:   str   = Query(..., description="End time (ISO 8601)"),
    resolution: Optional[str] = Query(None),
):
    """Return hourly surface current time series at (lat, lon) for a time range."""
    try:
        t_start = datetime.fromisoformat(start.replace("Z", "+00:00"))
        t_end   = datetime.fromisoformat(end.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(400, "Invalid time format.")

    if (t_end - t_start).total_seconds() > MAX_SERIES_HOURS * 3600:
        raise HTTPException(400, f"Time range exceeds maximum of {MAX_SERIES_HOURS} hours.")

    tables = [(r, tbl) for r, tbl in RESOLUTION_TABLES if resolution is None or r == resolution]

    conn = get_db()
    results = []
    try:
        with conn.cursor() as cur:
            for res, table in tables:
                if results and resolution is None:
                    break  # already have data from a finer resolution
                cur.execute(
                    f"""
                    SELECT DISTINCT ON (time_bucket('1 hour', time))
                        time, lat, lon, u, v
                    FROM {table}
                    WHERE time BETWEEN %s AND %s
                    ORDER BY time_bucket('1 hour', time),
                             geom <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                    """,
                    (t_start, t_end, lon, lat),
                )
                rows = cur.fetchall()
                if rows:
                    results = [row_to_dict(r, res) for r in rows]
    finally:
        conn.close()

    if not results:
        raise HTTPException(404, "No data found for this location and time range.")
    return results


@app.get("/currents/area")
@limiter.limit("15/minute")
def currents_area(
    request: Request,
    south: float = Query(...),
    north: float = Query(...),
    west:  float = Query(...),
    east:  float = Query(...),
    time:  str   = Query(..., description="UTC time (ISO 8601)"),
    resolution: str = Query("2km", description="Resolution: 500m, 1km, 2km, 6km"),
):
    """Return all valid surface current readings in a bounding box at a given time."""
    try:
        t = datetime.fromisoformat(time.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(400, "Invalid time format.")

    table_map = dict(RESOLUTION_TABLES)
    if resolution not in table_map:
        raise HTTPException(400, f"Unknown resolution. Use: {', '.join(table_map)}")
    table = table_map[resolution]

    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT lat, lon, u, v, time FROM {table}
                WHERE time BETWEEN %s AND %s
                  AND lat BETWEEN %s AND %s
                  AND lon BETWEEN %s AND %s
                LIMIT %s
                """,
                (t - timedelta(minutes=30),
                 t + timedelta(minutes=30),
                 south, north, west, east, MAX_AREA_POINTS),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        raise HTTPException(404, "No data found in this area and time.")
    return [row_to_dict(r, resolution) for r in rows]


# ── Drift prediction ───────────────────────────────────────────────────────────

class DriftRequest(BaseModel):
    lat: float
    lon: float
    time: str           # ISO 8601 UTC, e.g. "2026-04-10T14:00:00Z"
    hours: int = 24     # prediction window
    resolution: Optional[str] = None  # force a resolution; auto-selects finest if omitted


def _nearest_current(cur, lat, lon, t):
    """
    Return (u, v, resolution, data_lat, data_lon) for the nearest valid grid point
    to (lat, lon) at time t, trying resolutions finest → coarsest.
    Returns (0, 0, None, lat, lon) if no data found.
    """
    window = timedelta(minutes=35)   # ±35 min around the requested hour
    for res, table in RESOLUTION_TABLES:
        cur.execute(
            f"""
            SELECT u, v, lat, lon FROM {table}
            WHERE time BETWEEN %s AND %s
            ORDER BY geom <-> ST_SetSRID(ST_MakePoint(%s, %s), 4326)
            LIMIT 1
            """,
            (t - window, t + window, lon, lat),
        )
        row = cur.fetchone()
        if row:
            return float(row["u"]), float(row["v"]), res, float(row["lat"]), float(row["lon"])
    return 0.0, 0.0, None, lat, lon


@app.post("/predict/drift")
@limiter.limit("10/minute")
def predict_drift(request: Request, body: DriftRequest):
    """
    Dead-reckoning debris drift using archived HF radar surface currents.
    Steps hourly from the incident location, advecting by the nearest
    available surface current at each timestep.
    """
    if body.hours < 1 or body.hours > 120:
        raise HTTPException(400, "hours must be between 1 and 120.")

    try:
        start = datetime.fromisoformat(body.time.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(400, "Invalid time format. Use ISO 8601, e.g. 2026-04-10T14:00:00Z")

    conn = get_db()
    trajectory = []
    cur_lat, cur_lon = body.lat, body.lon
    warnings = []

    try:
        with conn.cursor() as cur:
            trajectory.append({
                "hour": 0,
                "time": start.isoformat(),
                "lat": round(cur_lat, 5),
                "lon": round(cur_lon, 5),
                "u_ms": None,
                "v_ms": None,
                "speed_ms": None,
                "direction_deg": None,
                "resolution": None,
                "no_data": False,
            })

            for h in range(body.hours):
                t = start + timedelta(hours=h)
                u, v, res, data_lat, data_lon = _nearest_current(cur, cur_lat, cur_lon, t)

                no_data = res is None
                if no_data and h == 0:
                    warnings.append("No HF radar data found near the incident location and time. "
                                    "Check that the date is within the archive (Jan 2026–present) "
                                    "and the location is on the US West Coast.")

                # Euler step: u=east (m/s), v=north (m/s) → displacement in degrees
                # 1 degree lat ≈ 111,000 m; 1 degree lon ≈ 111,000 * cos(lat) m
                dt = 3600  # seconds
                delta_lat = (v * dt) / 111_000
                delta_lon  = (u * dt) / (111_000 * math.cos(math.radians(cur_lat)))

                cur_lat += delta_lat
                cur_lon += delta_lon

                speed = math.sqrt(u**2 + v**2)
                direction = math.degrees(math.atan2(u, v)) % 360

                trajectory.append({
                    "hour": h + 1,
                    "time": (start + timedelta(hours=h + 1)).isoformat(),
                    "lat": round(cur_lat, 5),
                    "lon": round(cur_lon, 5),
                    "u_ms": round(u, 4),
                    "v_ms": round(v, 4),
                    "speed_ms": round(speed, 4),
                    "direction_deg": round(direction, 1),
                    "resolution": res,
                    "no_data": no_data,
                })
    finally:
        conn.close()

    return {
        "incident": {"lat": body.lat, "lon": body.lon, "time": start.isoformat()},
        "hours": body.hours,
        "trajectory": trajectory,
        "warnings": warnings,
    }
