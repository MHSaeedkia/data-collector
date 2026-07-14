CREATE DATABASE markets;

\c markets

-- create enum for status
DO $$ BEGIN
  CREATE TYPE subscription_status AS ENUM (
    'subscribe',
    'unsubscribe',
    'pending-subscribe',
    'pending-unsubscribe'
  );
EXCEPTION
  WHEN duplicate_object THEN
    RAISE NOTICE 'subscription_status enum already exists, skipping.';
END $$;

-- create table
CREATE TABLE IF NOT EXISTS currencies (
    id BIGSERIAL PRIMARY KEY,
    "name" VARCHAR(20) NOT NULL UNIQUE
);

-- create table
CREATE TABLE IF NOT EXISTS markets (
    id BIGSERIAL PRIMARY KEY,
    base_id BIGINT NOT NULL REFERENCES currencies(id) ON DELETE CASCADE ON UPDATE CASCADE,
    quote_id BIGINT NOT NULL REFERENCES currencies(id) ON DELETE CASCADE ON UPDATE CASCADE,
    price_precision INTEGER,
    quantity_precision INTEGER,
    display_price_precision INTEGER,
    display_quantity_precision INTEGER,
    CONSTRAINT unique_market UNIQUE (base_id, quote_id),
    CONSTRAINT chk_base_quote_diff CHECK (base_id <> quote_id),
    CONSTRAINT chk_price_precision_nonneg CHECK (price_precision IS NULL OR price_precision >= 0),
    CONSTRAINT chk_quantity_precision_nonneg CHECK (quantity_precision IS NULL OR quantity_precision >= 0),
    CONSTRAINT chk_display_price_precision_nonneg CHECK (display_price_precision IS NULL OR display_price_precision >= 0),
    CONSTRAINT chk_display_quantity_precision_nonneg CHECK (display_quantity_precision IS NULL OR display_quantity_precision >= 0)
);

-- create table
CREATE TABLE IF NOT EXISTS exchanges (
    id BIGSERIAL PRIMARY KEY,
    "name" VARCHAR(20) NOT NULL UNIQUE,
    label VARCHAR(20) NOT NULL
);

-- create table
CREATE TABLE IF NOT EXISTS exchange_markets (
    id BIGSERIAL PRIMARY KEY,
    exchange_id BIGINT NOT NULL REFERENCES exchanges(id) ON DELETE CASCADE ON UPDATE CASCADE,
    market VARCHAR(100) NOT NULL,
    market_id BIGINT NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
    status subscription_status NOT NULL DEFAULT 'unsubscribe',
    price_amount_rebase INT NOT NULL DEFAULT 0,
    volume_amount_rebase INT NOT NULL DEFAULT 0,
    CONSTRAINT unique_exchange_market UNIQUE (exchange_id, market)
);

-- indexes on foreign keys (Postgres does not create these automatically)
CREATE INDEX IF NOT EXISTS idx_markets_base_id ON markets(base_id);
CREATE INDEX IF NOT EXISTS idx_markets_quote_id ON markets(quote_id);
CREATE INDEX IF NOT EXISTS idx_exchange_markets_exchange_id ON exchange_markets(exchange_id);
CREATE INDEX IF NOT EXISTS idx_exchange_markets_market_id ON exchange_markets(market_id);