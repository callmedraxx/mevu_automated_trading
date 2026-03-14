CREATE TABLE IF NOT EXISTS crypto_markets (
    id TEXT PRIMARY KEY,
    slug TEXT NOT NULL,
    title TEXT NOT NULL,
    timeframe TEXT,
    asset TEXT,
    direction TEXT,
    up_price DOUBLE PRECISION,
    down_price DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    liquidity DOUBLE PRECISION,
    series_slug TEXT,
    start_date TEXT,
    end_date TEXT,
    start_time TEXT,
    up_clob_token_id TEXT,
    down_clob_token_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_crypto_markets_timeframe ON crypto_markets(timeframe);
CREATE INDEX IF NOT EXISTS idx_crypto_markets_direction ON crypto_markets(direction);
CREATE INDEX IF NOT EXISTS idx_crypto_markets_asset ON crypto_markets(asset);
CREATE INDEX IF NOT EXISTS idx_crypto_markets_series ON crypto_markets(series_slug);
