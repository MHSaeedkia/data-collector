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
CREATE TABLE IF NOT EXISTS markets (
    id BIGSERIAL PRIMARY KEY,
    base VARCHAR(20) NOT NULL,
    quote VARCHAR(20) NOT NULL,
    price_precision INTEGER,
    quantity_precision INTEGER,
    CONSTRAINT unique_market UNIQUE (base, quote)
);

-- create table
CREATE TABLE IF NOT EXISTS exchange_markets (
    id BIGSERIAL PRIMARY KEY,
    exchange VARCHAR(100) NOT NULL,
    market VARCHAR(100) NOT NULL,
    market_id BIGINT NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
    status subscription_status NOT NULL DEFAULT 'unsubscribe',
    CONSTRAINT unique_exchange_market UNIQUE (exchange, market)
);
