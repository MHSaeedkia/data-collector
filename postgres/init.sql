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


-- seed currencies
INSERT INTO currencies (id, "name") VALUES
(1, 'BTC'),
(2, 'ETH'),
(3, 'XRP'),
(4, 'TRX'),
(5, 'SOL'),
(6, 'DOT'),
(7, 'HYPE'),
(8, 'DOGE'),
(9, 'PEPE'),
(10, 'SUI'),
(11, 'ZEC'),
(12, 'XLM'),
(13, 'LINK'),
(14, 'AVAX'),
(15, 'PAXG'),
(16, 'XAUT'),
(17, 'NEAR'),
(18, 'UNI'),
(19, 'AAVE'),
(20, 'OKX'),
(21, 'ADA'),
(22, 'GRAM'),
(23, 'BNB'),
(24, 'WLD'),
(25, 'MNT'),
(26, 'SHIB'),
(27, 'BTT'),
(28, 'USDT'),
(29, 'IRT'),
(30, 'TMN');
SELECT setval(pg_get_serial_sequence('currencies', 'id'), (SELECT MAX(id) FROM currencies));

-- seed exchanges
INSERT INTO exchanges (id, name, label) VALUES
(1, 'nobitex', 'نوبیتکس'),
(2, 'bitpin', 'بیت پین'),
(3, 'wallex', 'والکس');
SELECT setval(pg_get_serial_sequence('exchanges', 'id'), (SELECT MAX(id) FROM exchanges));

-- seed markets
INSERT INTO markets (id, base_id, quote_id, price_precision, quantity_precision) VALUES
(1, 1, 28, 2, 8), -- BTC/USDT
(2, 1, 29, 2, 8), -- BTC/IRT
(3, 2, 28, 2, 8), -- ETH/USDT
(4, 2, 29, 2, 8), -- ETH/IRT
(5, 3, 28, 2, 8), -- XRP/USDT
(6, 3, 29, 2, 8), -- XRP/IRT
(7, 4, 28, 2, 8), -- TRX/USDT
(8, 4, 29, 2, 8), -- TRX/IRT
(9, 5, 28, 2, 8), -- SOL/USDT
(10, 5, 29, 2, 8), -- SOL/IRT
(11, 6, 28, 2, 8), -- DOT/USDT
(12, 6, 29, 2, 8), -- DOT/IRT
(13, 7, 28, 2, 8), -- HYPE/USDT
(14, 7, 29, 2, 8), -- HYPE/IRT
(15, 8, 28, 2, 8), -- DOGE/USDT
(16, 8, 29, 2, 8), -- DOGE/IRT
(17, 9, 28, 2, 8), -- PEPE/USDT
(18, 9, 29, 2, 8), -- PEPE/IRT
(19, 10, 28, 2, 8), -- SUI/USDT
(20, 10, 29, 2, 8), -- SUI/IRT
(21, 11, 28, 2, 8), -- ZEC/USDT
(22, 11, 29, 2, 8), -- ZEC/IRT
(23, 12, 28, 2, 8), -- XLM/USDT
(24, 12, 29, 2, 8), -- XLM/IRT
(25, 13, 28, 2, 8), -- LINK/USDT
(26, 13, 29, 2, 8), -- LINK/IRT
(27, 14, 28, 2, 8), -- AVAX/USDT
(28, 14, 29, 2, 8), -- AVAX/IRT
(29, 15, 28, 2, 8), -- PAXG/USDT
(30, 15, 29, 2, 8), -- PAXG/IRT
(31, 16, 28, 2, 8), -- XAUT/USDT
(32, 16, 29, 2, 8), -- XAUT/IRT
(33, 17, 28, 2, 8), -- NEAR/USDT
(34, 17, 29, 2, 8), -- NEAR/IRT
(35, 18, 28, 2, 8), -- UNI/USDT
(36, 18, 29, 2, 8), -- UNI/IRT
(37, 19, 28, 2, 8), -- AAVE/USDT
(38, 19, 29, 2, 8), -- AAVE/IRT
(39, 20, 28, 2, 8), -- OKX/USDT
(40, 20, 29, 2, 8), -- OKX/IRT
(41, 21, 28, 2, 8), -- ADA/USDT
(42, 21, 29, 2, 8), -- ADA/IRT
(43, 22, 28, 2, 8), -- GRAM/USDT
(44, 22, 29, 2, 8), -- GRAM/IRT
(45, 23, 28, 2, 8), -- BNB/USDT
(46, 23, 29, 2, 8), -- BNB/IRT
(47, 24, 28, 2, 8), -- WLD/USDT
(48, 24, 29, 2, 8), -- WLD/IRT
(49, 25, 28, 2, 8), -- MNT/USDT
(50, 25, 29, 2, 8), -- MNT/IRT
(51, 26, 28, 2, 8), -- SHIB/USDT
(52, 26, 29, 2, 8), -- SHIB/IRT
(53, 27, 28, 2, 8), -- BTT/USDT
(54, 27, 29, 2, 8), -- BTT/IRT
(55, 1, 30, 2, 8), -- BTC/TMN
(56, 2, 30, 2, 8), -- ETH/TMN
(57, 3, 30, 2, 8), -- XRP/TMN
(58, 4, 30, 2, 8), -- TRX/TMN
(59, 5, 30, 2, 8), -- SOL/TMN
(60, 6, 30, 2, 8), -- DOT/TMN
(61, 7, 30, 2, 8), -- HYPE/TMN
(62, 8, 30, 2, 8), -- DOGE/TMN
(63, 9, 30, 2, 8), -- PEPE/TMN
(64, 10, 30, 2, 8), -- SUI/TMN
(65, 11, 30, 2, 8), -- ZEC/TMN
(66, 12, 30, 2, 8), -- XLM/TMN
(67, 13, 30, 2, 8), -- LINK/TMN
(68, 14, 30, 2, 8), -- AVAX/TMN
(69, 15, 30, 2, 8), -- PAXG/TMN
(70, 16, 30, 2, 8), -- XAUT/TMN
(71, 17, 30, 2, 8), -- NEAR/TMN
(72, 18, 30, 2, 8), -- UNI/TMN
(73, 19, 30, 2, 8), -- AAVE/TMN
(74, 20, 30, 2, 8), -- OKX/TMN
(75, 21, 30, 2, 8), -- ADA/TMN
(76, 22, 30, 2, 8), -- GRAM/TMN
(77, 23, 30, 2, 8), -- BNB/TMN
(78, 24, 30, 2, 8), -- WLD/TMN
(79, 25, 30, 2, 8), -- MNT/TMN
(80, 26, 30, 2, 8), -- SHIB/TMN
(81, 27, 30, 2, 8); -- BTT/TMN
SELECT setval(pg_get_serial_sequence('markets', 'id'), (SELECT MAX(id) FROM markets));

-- seed exchange_markets
INSERT INTO exchange_markets (exchange_id, market, market_id, "status") VALUES
(1, 'BTCUSDT', 1, 'subscribe'), -- nobitex BTC/USDT
(1, 'BTCIRT', 2, 'unsubscribe'), -- nobitex BTC/IRT
(2, 'BTC_USDT', 1, 'subscribe'), -- bitpin  BTC/USDT
(2, 'BTC_IRT', 2, 'unsubscribe'), -- bitpin  BTC/IRT
(3, 'BTCUSDT', 1, 'unsubscribe'), -- wallex  BTC/USDT
(3, 'BTCTMN', 55, 'unsubscribe'), -- wallex  BTC/TMN
(1, 'ETHUSDT', 3, 'unsubscribe'), -- nobitex ETH/USDT
(1, 'ETHIRT', 4, 'unsubscribe'), -- nobitex ETH/IRT
(2, 'ETH_USDT', 3, 'unsubscribe'), -- bitpin  ETH/USDT
(2, 'ETH_IRT', 4, 'unsubscribe'), -- bitpin  ETH/IRT
(3, 'ETHUSDT', 3, 'unsubscribe'), -- wallex  ETH/USDT
(3, 'ETHTMN', 56, 'unsubscribe'), -- wallex  ETH/TMN
(1, 'XRPUSDT', 5, 'unsubscribe'), -- nobitex XRP/USDT
(1, 'XRPIRT', 6, 'unsubscribe'), -- nobitex XRP/IRT
(2, 'XRP_USDT', 5, 'unsubscribe'), -- bitpin  XRP/USDT
(2, 'XRP_IRT', 6, 'unsubscribe'), -- bitpin  XRP/IRT
(3, 'XRPUSDT', 5, 'unsubscribe'), -- wallex  XRP/USDT
(3, 'XRPTMN', 57, 'unsubscribe'), -- wallex  XRP/TMN
(1, 'TRXUSDT', 7, 'unsubscribe'), -- nobitex TRX/USDT
(1, 'TRXIRT', 8, 'unsubscribe'), -- nobitex TRX/IRT
(2, 'TRX_USDT', 7, 'unsubscribe'), -- bitpin  TRX/USDT
(2, 'TRX_IRT', 8, 'unsubscribe'), -- bitpin  TRX/IRT
(3, 'TRXUSDT', 7, 'unsubscribe'), -- wallex  TRX/USDT
(3, 'TRXTMN', 58, 'unsubscribe'), -- wallex  TRX/TMN
(1, 'SOLUSDT', 9, 'unsubscribe'), -- nobitex SOL/USDT
(1, 'SOLIRT', 10, 'unsubscribe'), -- nobitex SOL/IRT
(2, 'SOL_USDT', 9, 'unsubscribe'), -- bitpin  SOL/USDT
(2, 'SOL_IRT', 10, 'unsubscribe'), -- bitpin  SOL/IRT
(3, 'SOLUSDT', 9, 'unsubscribe'), -- wallex  SOL/USDT
(3, 'SOLTMN', 59, 'unsubscribe'), -- wallex  SOL/TMN
(1, 'DOTUSDT', 11, 'unsubscribe'), -- nobitex DOT/USDT
(1, 'DOTIRT', 12, 'unsubscribe'), -- nobitex DOT/IRT
(2, 'DOT_USDT', 11, 'unsubscribe'), -- bitpin  DOT/USDT
(2, 'DOT_IRT', 12, 'unsubscribe'), -- bitpin  DOT/IRT
(3, 'DOTUSDT', 11, 'unsubscribe'), -- wallex  DOT/USDT
(3, 'DOTTMN', 60, 'unsubscribe'), -- wallex  DOT/TMN
(1, 'HYPEUSDT', 13, 'unsubscribe'), -- nobitex HYPE/USDT
(1, 'HYPEIRT', 14, 'unsubscribe'), -- nobitex HYPE/IRT
(2, 'HYPE_USDT', 13, 'unsubscribe'), -- bitpin  HYPE/USDT
(2, 'HYPE_IRT', 14, 'unsubscribe'), -- bitpin  HYPE/IRT
(3, 'HYPEUSDT', 13, 'unsubscribe'), -- wallex  HYPE/USDT
(3, 'HYPETMN', 61, 'unsubscribe'), -- wallex  HYPE/TMN
(1, 'DOGEUSDT', 15, 'unsubscribe'), -- nobitex DOGE/USDT
(1, 'DOGEIRT', 16, 'unsubscribe'), -- nobitex DOGE/IRT
(2, 'DOGE_USDT', 15, 'unsubscribe'), -- bitpin  DOGE/USDT
(2, 'DOGE_IRT', 16, 'unsubscribe'), -- bitpin  DOGE/IRT
(3, 'DOGEUSDT', 15, 'unsubscribe'), -- wallex  DOGE/USDT
(3, 'DOGETMN', 62, 'unsubscribe'), -- wallex  DOGE/TMN
(1, 'PEPEUSDT', 17, 'unsubscribe'), -- nobitex PEPE/USDT
(1, 'PEPEIRT', 18, 'unsubscribe'), -- nobitex PEPE/IRT
(2, 'PEPE_USDT', 17, 'unsubscribe'), -- bitpin  PEPE/USDT
(2, 'PEPE_IRT', 18, 'unsubscribe'), -- bitpin  PEPE/IRT
(3, 'PEPEUSDT', 17, 'unsubscribe'), -- wallex  PEPE/USDT
(3, 'PEPETMN', 63, 'unsubscribe'), -- wallex  PEPE/TMN
(1, 'SUIUSDT', 19, 'unsubscribe'), -- nobitex SUI/USDT
(1, 'SUIIRT', 20, 'unsubscribe'), -- nobitex SUI/IRT
(2, 'SUI_USDT', 19, 'unsubscribe'), -- bitpin  SUI/USDT
(2, 'SUI_IRT', 20, 'unsubscribe'), -- bitpin  SUI/IRT
(3, 'SUIUSDT', 19, 'unsubscribe'), -- wallex  SUI/USDT
(3, 'SUITMN', 64, 'unsubscribe'), -- wallex  SUI/TMN
(1, 'ZECUSDT', 21, 'unsubscribe'), -- nobitex ZEC/USDT
(1, 'ZECIRT', 22, 'unsubscribe'), -- nobitex ZEC/IRT
(2, 'ZEC_USDT', 21, 'unsubscribe'), -- bitpin  ZEC/USDT
(2, 'ZEC_IRT', 22, 'unsubscribe'), -- bitpin  ZEC/IRT
(3, 'ZECUSDT', 21, 'unsubscribe'), -- wallex  ZEC/USDT
(3, 'ZECTMN', 65, 'unsubscribe'), -- wallex  ZEC/TMN
(1, 'XLMUSDT', 23, 'unsubscribe'), -- nobitex XLM/USDT
(1, 'XLMIRT', 24, 'unsubscribe'), -- nobitex XLM/IRT
(2, 'XLM_USDT', 23, 'unsubscribe'), -- bitpin  XLM/USDT
(2, 'XLM_IRT', 24, 'unsubscribe'), -- bitpin  XLM/IRT
(3, 'XLMUSDT', 23, 'unsubscribe'), -- wallex  XLM/USDT
(3, 'XLMTMN', 66, 'unsubscribe'), -- wallex  XLM/TMN
(1, 'LINKUSDT', 25, 'unsubscribe'), -- nobitex LINK/USDT
(1, 'LINKIRT', 26, 'unsubscribe'), -- nobitex LINK/IRT
(2, 'LINK_USDT', 25, 'unsubscribe'), -- bitpin  LINK/USDT
(2, 'LINK_IRT', 26, 'unsubscribe'), -- bitpin  LINK/IRT
(3, 'LINKUSDT', 25, 'unsubscribe'), -- wallex  LINK/USDT
(3, 'LINKTMN', 67, 'unsubscribe'), -- wallex  LINK/TMN
(1, 'AVAXUSDT', 27, 'unsubscribe'), -- nobitex AVAX/USDT
(1, 'AVAXIRT', 28, 'unsubscribe'), -- nobitex AVAX/IRT
(2, 'AVAX_USDT', 27, 'unsubscribe'), -- bitpin  AVAX/USDT
(2, 'AVAX_IRT', 28, 'unsubscribe'), -- bitpin  AVAX/IRT
(3, 'AVAXUSDT', 27, 'unsubscribe'), -- wallex  AVAX/USDT
(3, 'AVAXTMN', 68, 'unsubscribe'), -- wallex  AVAX/TMN
(1, 'PAXGUSDT', 29, 'unsubscribe'), -- nobitex PAXG/USDT
(1, 'PAXGIRT', 30, 'unsubscribe'), -- nobitex PAXG/IRT
(2, 'PAXG_USDT', 29, 'unsubscribe'), -- bitpin  PAXG/USDT
(2, 'PAXG_IRT', 30, 'unsubscribe'), -- bitpin  PAXG/IRT
(3, 'PAXGUSDT', 29, 'unsubscribe'), -- wallex  PAXG/USDT
(3, 'PAXGTMN', 69, 'unsubscribe'), -- wallex  PAXG/TMN
(1, 'XAUTUSDT', 31, 'unsubscribe'), -- nobitex XAUT/USDT
(1, 'XAUTIRT', 32, 'unsubscribe'), -- nobitex XAUT/IRT
(2, 'XAUT_USDT', 31, 'unsubscribe'), -- bitpin  XAUT/USDT
(2, 'XAUT_IRT', 32, 'unsubscribe'), -- bitpin  XAUT/IRT
(3, 'XAUTUSDT', 31, 'unsubscribe'), -- wallex  XAUT/USDT
(3, 'XAUTTMN', 70, 'unsubscribe'), -- wallex  XAUT/TMN
(1, 'NEARUSDT', 33, 'unsubscribe'), -- nobitex NEAR/USDT
(1, 'NEARIRT', 34, 'unsubscribe'), -- nobitex NEAR/IRT
(2, 'NEAR_USDT', 33, 'unsubscribe'), -- bitpin  NEAR/USDT
(2, 'NEAR_IRT', 34, 'unsubscribe'), -- bitpin  NEAR/IRT
(3, 'NEARUSDT', 33, 'unsubscribe'), -- wallex  NEAR/USDT
(3, 'NEARTMN', 71, 'unsubscribe'), -- wallex  NEAR/TMN
(1, 'UNIUSDT', 35, 'unsubscribe'), -- nobitex UNI/USDT
(1, 'UNIIRT', 36, 'unsubscribe'), -- nobitex UNI/IRT
(2, 'UNI_USDT', 35, 'unsubscribe'), -- bitpin  UNI/USDT
(2, 'UNI_IRT', 36, 'unsubscribe'), -- bitpin  UNI/IRT
(3, 'UNIUSDT', 35, 'unsubscribe'), -- wallex  UNI/USDT
(3, 'UNITMN', 72, 'unsubscribe'), -- wallex  UNI/TMN
(1, 'AAVEUSDT', 37, 'unsubscribe'), -- nobitex AAVE/USDT
(1, 'AAVEIRT', 38, 'unsubscribe'), -- nobitex AAVE/IRT
(2, 'AAVE_USDT', 37, 'unsubscribe'), -- bitpin  AAVE/USDT
(2, 'AAVE_IRT', 38, 'unsubscribe'), -- bitpin  AAVE/IRT
(3, 'AAVEUSDT', 37, 'unsubscribe'), -- wallex  AAVE/USDT
(3, 'AAVETMN', 73, 'unsubscribe'), -- wallex  AAVE/TMN
(1, 'OKXUSDT', 39, 'unsubscribe'), -- nobitex OKX/USDT
(1, 'OKXIRT', 40, 'unsubscribe'), -- nobitex OKX/IRT
(2, 'OKX_USDT', 39, 'unsubscribe'), -- bitpin  OKX/USDT
(2, 'OKX_IRT', 40, 'unsubscribe'), -- bitpin  OKX/IRT
(3, 'OKXUSDT', 39, 'unsubscribe'), -- wallex  OKX/USDT
(3, 'OKXTMN', 74, 'unsubscribe'), -- wallex  OKX/TMN
(1, 'ADAUSDT', 41, 'unsubscribe'), -- nobitex ADA/USDT
(1, 'ADAIRT', 42, 'unsubscribe'), -- nobitex ADA/IRT
(2, 'ADA_USDT', 41, 'unsubscribe'), -- bitpin  ADA/USDT
(2, 'ADA_IRT', 42, 'unsubscribe'), -- bitpin  ADA/IRT
(3, 'ADAUSDT', 41, 'unsubscribe'), -- wallex  ADA/USDT
(3, 'ADATMN', 75, 'unsubscribe'), -- wallex  ADA/TMN
(1, 'GRAMUSDT', 43, 'unsubscribe'), -- nobitex GRAM/USDT
(1, 'GRAMIRT', 44, 'unsubscribe'), -- nobitex GRAM/IRT
(2, 'GRAM_USDT', 43, 'unsubscribe'), -- bitpin  GRAM/USDT
(2, 'GRAM_IRT', 44, 'unsubscribe'), -- bitpin  GRAM/IRT
(3, 'GRAMUSDT', 43, 'unsubscribe'), -- wallex  GRAM/USDT
(3, 'GRAMTMN', 76, 'unsubscribe'), -- wallex  GRAM/TMN
(1, 'BNBUSDT', 45, 'unsubscribe'), -- nobitex BNB/USDT
(1, 'BNBIRT', 46, 'unsubscribe'), -- nobitex BNB/IRT
(2, 'BNB_USDT', 45, 'unsubscribe'), -- bitpin  BNB/USDT
(2, 'BNB_IRT', 46, 'unsubscribe'), -- bitpin  BNB/IRT
(3, 'BNBUSDT', 45, 'unsubscribe'), -- wallex  BNB/USDT
(3, 'BNBTMN', 77, 'unsubscribe'), -- wallex  BNB/TMN
(1, 'WLDUSDT', 47, 'unsubscribe'), -- nobitex WLD/USDT
(1, 'WLDIRT', 48, 'unsubscribe'), -- nobitex WLD/IRT
(2, 'WLD_USDT', 47, 'unsubscribe'), -- bitpin  WLD/USDT
(2, 'WLD_IRT', 48, 'unsubscribe'), -- bitpin  WLD/IRT
(3, 'WLDUSDT', 47, 'unsubscribe'), -- wallex  WLD/USDT
(3, 'WLDTMN', 78, 'unsubscribe'), -- wallex  WLD/TMN
(1, 'MNTUSDT', 49, 'unsubscribe'), -- nobitex MNT/USDT
(1, 'MNTIRT', 50, 'unsubscribe'), -- nobitex MNT/IRT
(2, 'MNT_USDT', 49, 'unsubscribe'), -- bitpin  MNT/USDT
(2, 'MNT_IRT', 50, 'unsubscribe'), -- bitpin  MNT/IRT
(3, 'MNTUSDT', 49, 'unsubscribe'), -- wallex  MNT/USDT
(3, 'MNTTMN', 79, 'unsubscribe'), -- wallex  MNT/TMN
(1, 'SHIBUSDT', 51, 'unsubscribe'), -- nobitex SHIB/USDT
(1, 'SHIBIRT', 52, 'unsubscribe'), -- nobitex SHIB/IRT
(2, 'SHIB_USDT', 51, 'unsubscribe'), -- bitpin  SHIB/USDT
(2, 'SHIB_IRT', 52, 'unsubscribe'), -- bitpin  SHIB/IRT
(3, 'SHIBUSDT', 51, 'unsubscribe'), -- wallex  SHIB/USDT
(3, 'SHIBTMN', 80, 'unsubscribe'), -- wallex  SHIB/TMN
(1, 'BTTUSDT', 53, 'unsubscribe'), -- nobitex BTT/USDT
(1, 'BTTIRT', 54, 'unsubscribe'), -- nobitex BTT/IRT
(2, 'BTT_USDT', 53, 'unsubscribe'), -- bitpin  BTT/USDT
(2, 'BTT_IRT', 54, 'unsubscribe'), -- bitpin  BTT/IRT
(3, 'BTTUSDT', 53, 'unsubscribe'), -- wallex  BTT/USDT
(3, 'BTTTMN', 81, 'unsubscribe'); -- wallex  BTT/TMN;