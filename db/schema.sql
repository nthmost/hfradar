-- HF Radar Surface Current Archive
-- Requires: PostgreSQL + TimescaleDB + PostGIS

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;

-- One table per resolution. Only non-NaN observations stored.
-- u = eastward velocity (m/s), v = northward velocity (m/s)
-- geom is a generated column for PostGIS KNN nearest-neighbor queries.

CREATE TABLE hfr_uswc_6km (
    time        TIMESTAMPTZ  NOT NULL,
    lat         FLOAT4       NOT NULL,
    lon         FLOAT4       NOT NULL,
    u           FLOAT4       NOT NULL,
    v           FLOAT4       NOT NULL,
    geom        GEOMETRY(Point, 4326) GENERATED ALWAYS AS
                    (ST_SetSRID(ST_MakePoint(lon, lat), 4326)) STORED
);
SELECT create_hypertable('hfr_uswc_6km', 'time', chunk_time_interval => INTERVAL '1 month');
CREATE INDEX ON hfr_uswc_6km USING GIST (geom);
CREATE INDEX ON hfr_uswc_6km (time DESC);

CREATE TABLE hfr_uswc_2km (
    time        TIMESTAMPTZ  NOT NULL,
    lat         FLOAT4       NOT NULL,
    lon         FLOAT4       NOT NULL,
    u           FLOAT4       NOT NULL,
    v           FLOAT4       NOT NULL,
    geom        GEOMETRY(Point, 4326) GENERATED ALWAYS AS
                    (ST_SetSRID(ST_MakePoint(lon, lat), 4326)) STORED
);
SELECT create_hypertable('hfr_uswc_2km', 'time', chunk_time_interval => INTERVAL '1 month');
CREATE INDEX ON hfr_uswc_2km USING GIST (geom);
CREATE INDEX ON hfr_uswc_2km (time DESC);

CREATE TABLE hfr_uswc_1km (
    time        TIMESTAMPTZ  NOT NULL,
    lat         FLOAT4       NOT NULL,
    lon         FLOAT4       NOT NULL,
    u           FLOAT4       NOT NULL,
    v           FLOAT4       NOT NULL,
    geom        GEOMETRY(Point, 4326) GENERATED ALWAYS AS
                    (ST_SetSRID(ST_MakePoint(lon, lat), 4326)) STORED
);
SELECT create_hypertable('hfr_uswc_1km', 'time', chunk_time_interval => INTERVAL '2 weeks');
CREATE INDEX ON hfr_uswc_1km USING GIST (geom);
CREATE INDEX ON hfr_uswc_1km (time DESC);

CREATE TABLE hfr_uswc_500m (
    time        TIMESTAMPTZ  NOT NULL,
    lat         FLOAT4       NOT NULL,
    lon         FLOAT4       NOT NULL,
    u           FLOAT4       NOT NULL,
    v           FLOAT4       NOT NULL,
    geom        GEOMETRY(Point, 4326) GENERATED ALWAYS AS
                    (ST_SetSRID(ST_MakePoint(lon, lat), 4326)) STORED
);
SELECT create_hypertable('hfr_uswc_500m', 'time', chunk_time_interval => INTERVAL '1 week');
CREATE INDEX ON hfr_uswc_500m USING GIST (geom);
CREATE INDEX ON hfr_uswc_500m (time DESC);

-- Tracks ingestion state per dataset so the collector knows where to resume.
CREATE TABLE hfr_ingest_log (
    dataset     TEXT         NOT NULL,
    last_time   TIMESTAMPTZ  NOT NULL,
    rows_added  INTEGER      NOT NULL DEFAULT 0,
    ingested_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dataset, last_time)
);

-- Columnstore (compression) policies: older chunks stored in compressed columnar format.
-- TimescaleDB 2.14+ uses add_columnstore_policy; enable per-table first.
ALTER TABLE hfr_uswc_6km  SET (timescaledb.compress, timescaledb.compress_orderby = 'time DESC');
ALTER TABLE hfr_uswc_2km  SET (timescaledb.compress, timescaledb.compress_orderby = 'time DESC');
ALTER TABLE hfr_uswc_1km  SET (timescaledb.compress, timescaledb.compress_orderby = 'time DESC');
ALTER TABLE hfr_uswc_500m SET (timescaledb.compress, timescaledb.compress_orderby = 'time DESC');

SELECT add_compression_policy('hfr_uswc_6km',  INTERVAL '90 days');
SELECT add_compression_policy('hfr_uswc_2km',  INTERVAL '90 days');
SELECT add_compression_policy('hfr_uswc_1km',  INTERVAL '30 days');
SELECT add_compression_policy('hfr_uswc_500m', INTERVAL '30 days');
