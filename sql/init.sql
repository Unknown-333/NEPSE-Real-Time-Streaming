-- =============================================================
-- NEPSE Real-Time Streaming Pipeline — TimescaleDB Schema
-- =============================================================
--
-- PURPOSE:
--     Initialize the TimescaleDB database with the OHLCV storage
--     schema. This script runs automatically on first container
--     boot via Docker's /docker-entrypoint-initdb.d/ mechanism.
--
-- WHY TimescaleDB (not plain PostgreSQL)?
--     1. Hypertables: Auto-partition by time — queries on recent
--        data skip old chunks entirely (100x faster for dashboards)
--     2. Continuous aggregates: Materialized views that auto-refresh
--        (e.g., 5-min candles from 1-min data with zero query cost)
--     3. Compression: 90%+ storage reduction for historical data
--     4. Full PostgreSQL compatibility: Use pgAdmin, psycopg2,
--        Grafana PostgreSQL plugin — no special drivers needed
--
-- TABLES:
--     ohlcv_1min    — 1-minute OHLCV candlestick data (hypertable)
--
-- CONTINUOUS AGGREGATES:
--     ohlcv_5min    — 5-minute rollup (auto-refreshed)
--
-- =============================================================

-- ── Enable TimescaleDB Extension ──
-- Must be first. Without this, CREATE MATERIALIZED VIEW (cagg)
-- and hypertable functions will fail.
CREATE EXTENSION IF NOT EXISTS timescaledb;


-- ═══════════════════════════════════════════════════════════════
-- 1-Minute OHLCV Table
-- ═══════════════════════════════════════════════════════════════
-- WHY this schema?
--   - window_start is the natural time axis (used by Grafana)
--   - window_end is kept for completeness but rarely queried
--   - vwap (volume-weighted avg price) is pre-computed in PySpark
--     to avoid expensive re-calculation at query time
--   - ingested_at tracks when the row was written to DB (for
--     debugging pipeline lag)
--   - UNIQUE constraint on (symbol, window_start) enables
--     ON CONFLICT upsert — making the sink idempotent
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS ohlcv_1min (
    symbol          TEXT            NOT NULL,
    window_start    TIMESTAMPTZ     NOT NULL,
    window_end      TIMESTAMPTZ     NOT NULL,
    open            DOUBLE PRECISION NOT NULL,
    high            DOUBLE PRECISION NOT NULL,
    low             DOUBLE PRECISION NOT NULL,
    close           DOUBLE PRECISION NOT NULL,
    volume          BIGINT          NOT NULL,
    turnover        DOUBLE PRECISION NOT NULL,
    trade_count     INTEGER         NOT NULL,
    vwap            DOUBLE PRECISION NOT NULL,
    sector          TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     DEFAULT NOW(),

    -- Composite unique constraint for idempotent upserts.
    -- The sink uses ON CONFLICT (symbol, window_start) DO UPDATE
    -- so re-processing the same Kafka messages is safe.
    UNIQUE (symbol, window_start)
);


-- ═══════════════════════════════════════════════════════════════
-- Convert to Hypertable
-- ═══════════════════════════════════════════════════════════════
-- WHY hypertable?
--   Plain PostgreSQL stores ALL rows in one big heap. As data grows
--   (65 symbols × 240 windows/day × 250 days = ~3.9M rows/year),
--   queries slow down because Postgres scans everything.
--
--   Hypertables auto-partition by time into "chunks" (default: 7 days).
--   A Grafana query for "last 1 hour" only touches the latest chunk
--   instead of scanning the entire table.
--
-- WHY migrate_data => true?
--   If any rows were inserted before this line (shouldn't happen on
--   first boot, but defensive coding), they'll be migrated into chunks.
--
-- WHY if_not_exists => true?
--   Makes this script idempotent — safe to re-run without errors.
-- ═══════════════════════════════════════════════════════════════

SELECT create_hypertable(
    'ohlcv_1min',
    'window_start',
    if_not_exists    => TRUE,
    migrate_data     => TRUE
);


-- ═══════════════════════════════════════════════════════════════
-- Indexes
-- ═══════════════════════════════════════════════════════════════
-- WHY these indexes?
--   1. (symbol, window_start DESC): The #1 query pattern.
--      Grafana asks: "Give me NABIL's candles for the last hour"
--      → needs fast lookup by symbol + time range, newest first.
--
--   2. (sector, window_start DESC): For sector-level dashboards.
--      "Show me all Commercial Bank stocks in the last 30 mins"
--
-- WHY DESC on window_start?
--   Dashboards always show most recent data first. DESC index
--   means the DB reads the index forward (fast) instead of
--   backward (slower) for ORDER BY window_start DESC queries.
-- ═══════════════════════════════════════════════════════════════

CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_time
    ON ohlcv_1min (symbol, window_start DESC);

CREATE INDEX IF NOT EXISTS idx_ohlcv_sector_time
    ON ohlcv_1min (sector, window_start DESC);


-- ═══════════════════════════════════════════════════════════════
-- 5-Minute Continuous Aggregate
-- ═══════════════════════════════════════════════════════════════
-- WHY continuous aggregates?
--   Without this, a "5-minute candle" query would:
--     1. Scan all 1-min rows in the time range
--     2. Re-compute OHLCV aggregation on the fly
--     3. Repeat this every 10 seconds (Grafana auto-refresh)
--
--   Continuous aggregates pre-compute and cache the results.
--   Grafana reads the materialized view = instant response.
--   TimescaleDB auto-refreshes it as new 1-min data arrives.
--
-- NOTE on OHLCV rollup:
--   - open = first 1-min candle's open (first open in period)
--   - close = last 1-min candle's close (last close in period)
--   - high/low = max/min across all 1-min candles
--   - volume/turnover = SUM across all 1-min candles
-- ═══════════════════════════════════════════════════════════════

CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_5min
WITH (timescaledb.continuous) AS
SELECT
    symbol,
    time_bucket('5 minutes', window_start)  AS window_start,
    FIRST(open, window_start)               AS open,
    MAX(high)                               AS high,
    MIN(low)                                AS low,
    LAST(close, window_start)               AS close,
    SUM(volume)                             AS volume,
    SUM(turnover)                           AS turnover,
    SUM(trade_count)                        AS trade_count,
    SUM(turnover) / NULLIF(SUM(volume), 0)  AS vwap,
    FIRST(sector, window_start)             AS sector
FROM ohlcv_1min
GROUP BY symbol, time_bucket('5 minutes', window_start)
WITH NO DATA;


-- ═══════════════════════════════════════════════════════════════
-- Continuous Aggregate Refresh Policy
-- ═══════════════════════════════════════════════════════════════
-- Auto-refresh the 5-min aggregate:
--   - start_offset: look back 1 hour (catches late data)
--   - end_offset: up to 5 minutes ago (most recent complete bucket)
--   - schedule_interval: every 5 minutes
-- ═══════════════════════════════════════════════════════════════

SELECT add_continuous_aggregate_policy('ohlcv_5min',
    start_offset    => INTERVAL '1 hour',
    end_offset      => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists   => TRUE
);


-- ═══════════════════════════════════════════════════════════════
-- Verification
-- ═══════════════════════════════════════════════════════════════

DO $$
BEGIN
    RAISE NOTICE '==========================================';
    RAISE NOTICE '  TimescaleDB Schema Initialized!';
    RAISE NOTICE '  Table:     ohlcv_1min (hypertable)';
    RAISE NOTICE '  Aggregate: ohlcv_5min (5-min rollup)';
    RAISE NOTICE '  Indexes:   symbol+time, sector+time';
    RAISE NOTICE '==========================================';
END
$$;
