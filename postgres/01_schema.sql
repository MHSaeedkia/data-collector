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
    base_id BIGINT NOT NULL REFERENCES currencies(id) ON DELETE RESTRICT ON UPDATE CASCADE,
    quote_id BIGINT NOT NULL REFERENCES currencies(id) ON DELETE RESTRICT ON UPDATE CASCADE,
    price_precision INTEGER,
    quantity_precision INTEGER,
    CONSTRAINT unique_market UNIQUE (base_id, quote_id)
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
    CONSTRAINT unique_exchange_market UNIQUE (exchange_id, market)
);