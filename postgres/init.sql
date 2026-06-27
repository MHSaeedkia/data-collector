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

INSERT INTO exchanges (id, name, label) values (1, 'nobitex', 'نوبیتکس'),(2, 'bitpin', 'بیت پین');
SELECT setval(pg_get_serial_sequence('exchanges', 'id'), (SELECT MAX(id) FROM exchanges));

INSERT INTO markets (id, base, quote, price_precision, quantity_precision) values
(1, 'BTC', 'USDT', 9, 9),
(2, 'ETH', 'USDT', 10, 10);
SELECT setval(pg_get_serial_sequence('markets', 'id'), (SELECT MAX(id) FROM markets));

INSERT INTO exchange_markets (exchange_id, market, market_id, "status")
values
(1, 'BTCUSDT', 1, 'subscribe'),
(1, 'ETHUSDT', 2, 'subscribe'),
(2, 'BTC_USDT', 1, 'subscribe'),
(2, 'ETH_USDT', 2, 'subscribe');

