# hfradar

**HF Radar Surface Current Archive and API**

A public service archiving US West Coast HF radar surface current data beyond the standard 90-day rolling window available from UCSD/SCCOOS. Data sourced from [NOAA CoastWatch ERDDAP](https://coastwatch.pfeg.noaa.gov/erddap/), archived continuously since service launch.

**Live at:** [hfr.nthmost.com](https://hfr.nthmost.com) / [hfr.nthmost.net](https://hfr.nthmost.net)

## Why this exists

NOAA's HF radar network measures true surface currents (0–2m depth) at high spatial resolution across the US West Coast. The data is excellent — but the public THREDDS/ERDDAP servers only retain the last ~90 days. This archive extends that window indefinitely, making the data useful for:

- Drift prediction and search-and-rescue planning
- Oceanographic research requiring long time series
- Training and validating coastal circulation models
- Environmental impact assessments

## Data

| Resolution | Coverage | Update frequency |
|---|---|---|
| 6km | Full US West Coast + offshore | Hourly |
| 2km | Inshore US West Coast | Hourly |
| 1km | Coastal US West Coast | Hourly |
| 500m | SF Bay, LA Basin, select hotspots | Hourly |

Source: UCSD HFRnet via [NOAA CoastWatch ERDDAP](https://coastwatch.pfeg.noaa.gov/erddap/griddap/ucsdHfrW2.html)

## API

### Point query

```
GET /currents/point?lat=37.8&lon=-122.5&time=2026-03-10T14:00:00Z
```

Returns the nearest valid surface current reading to the given coordinates.

```json
{
  "time": "2026-03-10T14:00:00+00:00",
  "lat": 37.8016,
  "lon": -122.4893,
  "u_ms": -0.21,
  "v_ms": 0.0,
  "speed_ms": 0.21,
  "direction_deg": 270.0,
  "resolution": "2km"
}
```

### Time series

```
GET /currents/series?lat=37.8&lon=-122.5&start=2026-03-10T00:00:00Z&end=2026-03-11T00:00:00Z
```

Returns hourly readings at the nearest grid point for the time range (max 30 days).

### Area query

```
GET /currents/area?south=37.6&north=38.1&west=-123.0&east=-122.2&time=2026-03-10T14:00:00Z&resolution=2km
```

Returns all valid readings in the bounding box at the given time.

### Status

```
GET /status
```

Returns data coverage summary (earliest/latest time, row counts) per resolution.

## Stack

- **PostgreSQL + TimescaleDB + PostGIS** — time-series storage with spatial indexing
- **FastAPI** — API server
- **Apache** — reverse proxy on zephyr (nthmost.com)
- **Collector** — hourly cron job pulling from ERDDAP, backfills on first run

## Self-hosting

```bash
# 1. Set up database
psql -d yourdb -f db/schema.sql

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure
export HFR_DATABASE_URL="postgresql://user:pass@localhost/hfradar"

# 4. Initial backfill (runs collector for all datasets, last 90 days)
python3 collector/collect.py --backfill-days 90

# 5. Run API
uvicorn api.main:app --host 0.0.0.0 --port 8042

# 6. Schedule hourly collection
# Add to crontab:
# 5 * * * * HFR_DATABASE_URL=... python3 /path/to/collector/collect.py
```

## License

Data is sourced from NOAA (public domain). Code: MIT.
