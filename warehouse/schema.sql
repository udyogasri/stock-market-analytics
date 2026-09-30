-- ==============================================================================
-- Amazon Redshift Warehouse Schema for Stock Market Analytics (Phase 5)
-- ==============================================================================

-- 1. 5-Minute Windowed Aggregations
CREATE TABLE IF NOT EXISTS agg_5min (
    ticker VARCHAR(16) NOT NULL,
    company_name VARCHAR(128),
    sector VARCHAR(64),
    exchange VARCHAR(16),
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume BIGINT NOT NULL,
    trade_count BIGINT NOT NULL
)
DISTSTYLE AUTO
SORTKEY (window_start, ticker);

-- 2. 1-Hour Windowed Aggregations
CREATE TABLE IF NOT EXISTS agg_1hour (
    ticker VARCHAR(16) NOT NULL,
    company_name VARCHAR(128),
    sector VARCHAR(64),
    exchange VARCHAR(16),
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume BIGINT NOT NULL,
    trade_count BIGINT NOT NULL
)
DISTSTYLE AUTO
SORTKEY (window_start, ticker);

-- 3. Warehouse Ingestion Audit Trail
CREATE TABLE IF NOT EXISTS load_audit (
    run_id VARCHAR(64) NOT NULL,
    table_name VARCHAR(64) NOT NULL,
    row_count BIGINT NOT NULL,
    loaded_at TIMESTAMP DEFAULT SYSDATE,
    status VARCHAR(32) NOT NULL
);
