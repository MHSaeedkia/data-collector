\c markets

-- seed exchanges
INSERT INTO exchanges (id, name, label) VALUES
(1, 'nobitex', 'نوبیتکس'),
(2, 'bitpin', 'بیت پین'),
(3, 'wallex', 'والکس'),
(4, 'ramzinex', 'رمزینکس'),
(5, 'bitget', 'بیت گت'),
(6, 'bybit', 'بای بیت'),
(7, 'ompfinex', 'او ام پی فینکس'),
(8, 'okx', 'اوکی ایکس'),
(9, 'lbank', 'ال بانک');
SELECT setval(pg_get_serial_sequence('exchanges', 'id'), (SELECT MAX(id) FROM exchanges));

-- ===== currencies =====
INSERT INTO currencies (id, "name") VALUES
(1, 'USDT'),
(2, 'IRT'),
(3, 'BTC'),
(4, 'ETH'),
(5, 'XRP'),
(6, 'TRX'),
(7, 'SOL'),
(8, 'DOT'),
(9, 'HYPE'),
(10, 'DOGE'),
(11, 'PEPE'),
(12, 'SUI'),
(13, 'ZEC'),
(14, 'XLM'),
(15, 'LINK'),
(16, 'AVAX'),
(17, 'PAXG'),
(18, 'XAUT'),
(19, 'NEAR'),
(20, 'UNI'),
(21, 'AAVE'),
(22, 'OKX'),
(23, 'ADA'),
(24, 'GRAM'),
(25, 'BNB'),
(26, 'WLD'),
(27, 'MNT'),
(28, 'SHIB'),
(29, 'BTT'),
(30, 'TON');
SELECT setval(pg_get_serial_sequence('currencies', 'id'), (SELECT MAX(id) FROM currencies));

-- ===== markets =====
INSERT INTO markets (id, base_id, quote_id, price_precision, quantity_precision, display_price_precision, display_quantity_precision) VALUES
(1, 3, 1, 2, 8, 2, 8), -- BTC/USDT
(2, 3, 2, 2, 8, 2, 8), -- BTC/IRT
(3, 4, 1, 2, 8, 2, 8), -- ETH/USDT
(4, 4, 2, 2, 8, 2, 8), -- ETH/IRT
(5, 5, 1, 2, 8, 2, 8), -- XRP/USDT
(6, 5, 2, 2, 8, 2, 8), -- XRP/IRT
(7, 6, 1, 2, 8, 2, 8), -- TRX/USDT
(8, 6, 2, 2, 8, 2, 8), -- TRX/IRT
(9, 7, 1, 2, 8, 2, 8), -- SOL/USDT
(10, 7, 2, 2, 8, 2, 8), -- SOL/IRT
(11, 8, 1, 2, 8, 2, 8), -- DOT/USDT
(12, 8, 2, 2, 8, 2, 8), -- DOT/IRT
(13, 9, 1, 2, 8, 2, 8), -- HYPE/USDT
(14, 9, 2, 2, 8, 2, 8), -- HYPE/IRT
(15, 10, 1, 2, 8, 2, 8), -- DOGE/USDT
(16, 10, 2, 2, 8, 2, 8), -- DOGE/IRT
(17, 11, 1, 10, 10, 2, 8), -- PEPE/USDT
(18, 11, 2, 10, 10, 2, 8), -- PEPE/IRT
(19, 12, 1, 2, 8, 2, 8), -- SUI/USDT
(20, 12, 2, 2, 8, 2, 8), -- SUI/IRT
(21, 13, 1, 2, 8, 2, 8), -- ZEC/USDT
(22, 13, 2, 2, 8, 2, 8), -- ZEC/IRT
(23, 14, 1, 2, 8, 2, 8), -- XLM/USDT
(24, 14, 2, 2, 8, 2, 8), -- XLM/IRT
(25, 15, 1, 2, 8, 2, 8), -- LINK/USDT
(26, 15, 2, 2, 8, 2, 8), -- LINK/IRT
(27, 16, 1, 2, 8, 2, 8), -- AVAX/USDT
(28, 16, 2, 2, 8, 2, 8), -- AVAX/IRT
(29, 17, 1, 2, 8, 2, 8), -- PAXG/USDT
(30, 17, 2, 2, 8, 2, 8), -- PAXG/IRT
(31, 18, 1, 2, 8, 2, 8), -- XAUT/USDT
(32, 18, 2, 2, 8, 2, 8), -- XAUT/IRT
(33, 19, 1, 2, 8, 2, 8), -- NEAR/USDT
(34, 19, 2, 2, 8, 2, 8), -- NEAR/IRT
(35, 20, 1, 2, 8, 2, 8), -- UNI/USDT
(36, 20, 2, 2, 8, 2, 8), -- UNI/IRT
(37, 21, 1, 2, 8, 2, 8), -- AAVE/USDT
(38, 21, 2, 2, 8, 2, 8), -- AAVE/IRT
(39, 22, 1, 2, 8, 2, 8), -- OKX/USDT
(40, 22, 2, 2, 8, 2, 8), -- OKX/IRT
(41, 23, 1, 2, 8, 2, 8), -- ADA/USDT
(42, 23, 2, 2, 8, 2, 8), -- ADA/IRT
(43, 24, 1, 2, 8, 2, 8), -- GRAM/USDT
(44, 24, 2, 2, 8, 2, 8), -- GRAM/IRT
(45, 25, 1, 2, 8, 2, 8), -- BNB/USDT
(46, 25, 2, 2, 8, 2, 8), -- BNB/IRT
(47, 26, 1, 2, 8, 2, 8), -- WLD/USDT
(48, 26, 2, 2, 8, 2, 8), -- WLD/IRT
(49, 27, 1, 2, 8, 2, 8), -- MNT/USDT
(50, 27, 2, 2, 8, 2, 8), -- MNT/IRT
(51, 28, 1, 2, 8, 2, 8), -- SHIB/USDT
(52, 28, 2, 2, 8, 2, 8), -- SHIB/IRT
(53, 29, 1, 2, 8, 2, 8), -- BTT/USDT
(54, 29, 2, 2, 8, 2, 8), -- BTT/IRT
(55, 30, 1, 2, 8, 2, 8), -- TON/USDT
(56, 30, 2, 2, 8, 2, 8); -- TON/IRT
SELECT setval(pg_get_serial_sequence('markets', 'id'), (SELECT MAX(id) FROM markets));

-- ===== exchange_markets =====
INSERT INTO exchange_markets (exchange_id, market, market_id, "status", price_amount_rebase, volume_amount_rebase, depth_aggregation_key, staleness_threshold_seconds) VALUES
(1, 'BTCUSDT', 1, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'BTCIRT', 2, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, 'ETHUSDT', 3, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'ETHIRT', 4, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, 'XRPUSDT', 5, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'XRPIRT', 6, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, 'TRXUSDT', 7, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'TRXIRT', 8, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, 'SOLUSDT', 9, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'SOLIRT', 10, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, 'DOTUSDT', 11, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'DOTIRT', 12, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, 'HYPEUSDT', 13, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'HYPEIRT', 14, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, 'DOGEUSDT', 15, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'DOGEIRT', 16, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, '1M_PEPEUSDT', 17, 'unsubscribe', -6, 6, NULL, 60), -- nobitex
(1, '1M_PEPEIRT', 18, 'unsubscribe', -7, 6, NULL, 60), -- nobitex
(1, 'SUIUSDT', 19, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'SUIIRT', 20, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, 'ZECUSDT', 21, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'ZECIRT', 22, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, 'XLMUSDT', 23, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'XLMIRT', 24, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, 'LINKUSDT', 25, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'LINKIRT', 26, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, 'AVAXUSDT', 27, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'AVAXIRT', 28, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, 'PAXGUSDT', 29, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'PAXGIRT', 30, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, 'XAUTUSDT', 31, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'XAUTIRT', 32, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, 'NEARUSDT', 33, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'NEARIRT', 34, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, 'UNIUSDT', 35, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'UNIIRT', 36, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, 'AAVEUSDT', 37, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'AAVEIRT', 38, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, 'ADAUSDT', 41, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'ADAIRT', 42, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, 'GRAMUSDT', 43, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'GRAMIRT', 44, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, 'BNBUSDT', 45, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'BNBIRT', 46, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, 'WLDUSDT', 47, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, 'WLDIRT', 48, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, '1K_SHIBUSDT', 51, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, '1K_SHIBIRT', 52, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(1, '1M_BTTUSDT', 53, 'unsubscribe', 0, 0, NULL, 60), -- nobitex
(1, '1M_BTTIRT', 54, 'unsubscribe', -1, 0, NULL, 60), -- nobitex
(2, 'BTC_USDT', 1, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'BTC_IRT', 2, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'ETH_USDT', 3, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'ETH_IRT', 4, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'XRP_USDT', 5, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'XRP_IRT', 6, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'TRX_USDT', 7, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'TRX_IRT', 8, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'SOL_USDT', 9, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'SOL_IRT', 10, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'DOT_USDT', 11, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'DOT_IRT', 12, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'HYPE_USDT', 13, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'HYPE_IRT', 14, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'DOGE_USDT', 15, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'DOGE_IRT', 16, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'PEPE_IRT', 18, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'SUI_USDT', 19, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'SUI_IRT', 20, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'ZEC_USDT', 21, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'ZEC_IRT', 22, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'XLM_USDT', 23, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'XLM_IRT', 24, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'LINK_USDT', 25, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'LINK_IRT', 26, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'AVAX_USDT', 27, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'AVAX_IRT', 28, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'PAXG_USDT', 29, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'PAXG_IRT', 30, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'XAUT_USDT', 31, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'XAUT_IRT', 32, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'NEAR_USDT', 33, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'NEAR_IRT', 34, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'UNI_USDT', 35, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'UNI_IRT', 36, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'AAVE_USDT', 37, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'AAVE_IRT', 38, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'ADA_USDT', 41, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'ADA_IRT', 42, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'GRAM_USDT', 43, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'GRAM_IRT', 44, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'BNB_USDT', 45, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'BNB_IRT', 46, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'WLD_USDT', 47, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'WLD_IRT', 48, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'MNT_USDT', 49, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'MNT_IRT', 50, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'SHIB_IRT', 52, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(2, 'BTTC_IRT', 54, 'unsubscribe', 0, 0, NULL, 60), -- bitpin
(3, 'BTCUSDT', 1, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'BTCTMN', 2, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'ETHUSDT', 3, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'ETHTMN', 4, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'XRPUSDT', 5, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'XRPTMN', 6, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'TRXUSDT', 7, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'TRXTMN', 8, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'SOLUSDT', 9, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'SOLTMN', 10, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'DOTUSDT', 11, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'DOTTMN', 12, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'HYPEUSDT', 13, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'HYPETMN', 14, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'DOGEUSDT', 15, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'DOGETMN', 16, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'PEPEUSDT', 17, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'PEPETMN', 18, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'SUIUSDT', 19, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'SUITMN', 20, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'ZECUSDT', 21, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'ZECTMN', 22, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'XLMUSDT', 23, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'XLMTMN', 24, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'LINKUSDT', 25, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'LINKTMN', 26, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'AVAXUSDT', 27, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'AVAXTMN', 28, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'PAXGUSDT', 29, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'PAXGTMN', 30, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'XAUTUSDT', 31, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'XAUTTMN', 32, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'NEARUSDT', 33, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'NEARTMN', 34, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'UNIUSDT', 35, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'UNITMN', 36, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'AAVEUSDT', 37, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'AAVETMN', 38, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'ADAUSDT', 41, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'ADATMN', 42, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'GRAMUSDT', 43, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'GRAMTMN', 44, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'BNBUSDT', 45, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'BNBTMN', 46, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'WLDUSDT', 47, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'WLDTMN', 48, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'MNTUSDT', 49, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'MNTTMN', 50, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'SHIBUSDT', 51, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'SHIBTMN', 52, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'BTTCUSDT', 53, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(3, 'BTTCTMN', 54, 'unsubscribe', 0, 0, NULL, 60), -- wallex
(4, '12', 1, 'unsubscribe', 0, 0, NULL, 60), -- ramzinex
(4, '2', 2, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '13', 3, 'unsubscribe', 0, 0, NULL, 60), -- ramzinex
(4, '3', 4, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '643', 5, 'unsubscribe', 0, 0, NULL, 60), -- ramzinex
(4, '4', 6, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '27', 7, 'unsubscribe', 0, 0, NULL, 60), -- ramzinex
(4, '25', 8, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '218', 9, 'unsubscribe', 0, 0, NULL, 60), -- ramzinex
(4, '96', 10, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '31', 11, 'unsubscribe', 0, 0, NULL, 60), -- ramzinex
(4, '29', 12, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '732', 13, 'unsubscribe', 0, 0, NULL, 60), -- ramzinex
(4, '731', 14, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '432', 15, 'unsubscribe', 0, 0, NULL, 60), -- ramzinex
(4, '10', 16, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '552', 17, 'unsubscribe', -2, 2, NULL, 60), -- ramzinex
(4, '366', 18, 'unsubscribe', -3, 2, NULL, 60), -- ramzinex
(4, '700', 19, 'unsubscribe', 0, 0, NULL, 60), -- ramzinex
(4, '699', 20, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '451', 21, 'unsubscribe', 0, 0, NULL, 60), -- ramzinex
(4, '291', 22, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '19', 24, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '38', 25, 'unsubscribe', 0, 0, NULL, 60), -- ramzinex
(4, '26', 26, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '741', 27, 'unsubscribe', 0, 0, NULL, 60), -- ramzinex
(4, '255', 28, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '881', 29, 'unsubscribe', 0, 0, NULL, 60), -- ramzinex
(4, '296', 30, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '899', 31, 'unsubscribe', 0, 0, NULL, 60), -- ramzinex
(4, '896', 32, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '267', 34, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '35', 35, 'unsubscribe', 0, 0, NULL, 60), -- ramzinex
(4, '34', 36, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '37', 37, 'unsubscribe', 0, 0, NULL, 60), -- ramzinex
(4, '36', 38, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '158', 41, 'unsubscribe', 0, 0, NULL, 60), -- ramzinex
(4, '33', 42, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '434', 43, 'unsubscribe', 0, 0, NULL, 60), -- ramzinex
(4, '272', 44, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '18', 45, 'unsubscribe', 0, 0, NULL, 60), -- ramzinex
(4, '17', 46, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '642', 47, 'unsubscribe', 0, 0, NULL, 60), -- ramzinex
(4, '376', 48, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '344', 50, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '518', 51, 'unsubscribe', 0, 0, NULL, 60), -- ramzinex
(4, '61', 52, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(4, '39', 54, 'unsubscribe', -1, 0, NULL, 60), -- ramzinex
(5, 'BTCUSDT', 1, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'ETHUSDT', 3, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'XRPUSDT', 5, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'TRXUSDT', 7, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'SOLUSDT', 9, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'DOTUSDT', 11, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'HYPEUSDT', 13, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'DOGEUSDT', 15, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'PEPEUSDT', 17, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'SUIUSDT', 19, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'ZECUSDT', 21, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'XLMUSDT', 23, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'LINKUSDT', 25, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'AVAXUSDT', 27, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'PAXGUSDT', 29, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'XAUTUSDT', 31, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'NEARUSDT', 33, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'UNIUSDT', 35, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'AAVEUSDT', 37, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'ADAUSDT', 41, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'GRAMUSDT', 43, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'BNBUSDT', 45, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'WLDUSDT', 47, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'MNTUSDT', 49, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'SHIBUSDT', 51, 'unsubscribe', 0, 0, 1, 60), -- bitget
(5, 'BTTUSDT', 53, 'unsubscribe', 0, 0, 1, 60), -- bitget
(6, 'BTCUSDT', 1, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'ETHUSDT', 3, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'XRPUSDT', 5, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'TRXUSDT', 7, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'SOLUSDT', 9, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'DOTUSDT', 11, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'HYPEUSDT', 13, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'DOGEUSDT', 15, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'PEPEUSDT', 17, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'SUIUSDT', 19, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'ZECUSDT', 21, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'XLMUSDT', 23, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'LINKUSDT', 25, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'AVAXUSDT', 27, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'PAXGUSDT', 29, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'XAUTUSDT', 31, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'NEARUSDT', 33, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'UNIUSDT', 35, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'AAVEUSDT', 37, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'OKXUSDT', 39, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'ADAUSDT', 41, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'GRAMUSDT', 43, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'BNBUSDT', 45, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'WLDUSDT', 47, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'MNTUSDT', 49, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'SHIBUSDT', 51, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(6, 'BTTUSDT', 53, 'unsubscribe', 0, 0, NULL, 60), -- bybit
(7, '14', 1, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '1', 2, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '15', 3, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '2', 4, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '17', 5, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '4', 6, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '21', 7, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '8', 8, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '28', 9, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '27', 10, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '63', 11, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '62', 12, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '357', 13, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '358', 14, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '18', 15, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '5', 16, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '168', 17, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '167', 18, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '295', 19, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '296', 20, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '23', 23, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '11', 24, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '77', 25, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '76', 26, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '156', 27, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '155', 28, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '134', 29, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '133', 30, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '136', 33, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '135', 34, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '112', 35, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '111', 36, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '71', 37, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '70', 38, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '22', 41, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '10', 42, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '19', 45, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '6', 46, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '266', 47, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '267', 48, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '403', 49, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '404', 50, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '31', 51, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '30', 52, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '196', 53, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '195', 54, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(7, '142', 55, 'unsubscribe', 0, 0, NULL, 60), -- ompfinex
(7, '141', 56, 'unsubscribe', -1, 0, NULL, 60), -- ompfinex
(8, 'BTC-USDT', 1, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'ETH-USDT', 3, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'XRP-USDT', 5, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'TRX-USDT', 7, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'SOL-USDT', 9, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'DOT-USDT', 11, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'HYPE-USDT', 13, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'DOGE-USDT', 15, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'PEPE-USDT', 17, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'SUI-USDT', 19, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'ZEC-USDT', 21, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'XLM-USDT', 23, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'LINK-USDT', 25, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'AVAX-USDT', 27, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'PAXG-USDT', 29, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'XAUT-USDT', 31, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'NEAR-USDT', 33, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'UNI-USDT', 35, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'AAVE-USDT', 37, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'OKX-USDT', 39, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'ADA-USDT', 41, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'GRAM-USDT', 43, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'BNB-USDT', 45, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'WLD-USDT', 47, 'unsubscribe', 0, 0, 1, 60), -- okx
(8, 'SHIB-USDT', 51, 'unsubscribe', 0, 0, 1, 60), -- okx
(9, 'btc_usdt', 1, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'eth_usdt', 3, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'xrp_usdt', 5, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'trx_usdt', 7, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'sol_usdt', 9, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'dot_usdt', 11, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'hype_usdt', 13, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'doge_usdt', 15, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'pepe_usdt', 17, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'sui_usdt', 19, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'zec_usdt', 21, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'xlm_usdt', 23, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'link_usdt', 25, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'avax_usdt', 27, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'paxg_usdt', 29, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'xaut_usdt', 31, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'near_usdt', 33, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'uni_usdt', 35, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'aave_usdt', 37, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'okb_usdt', 39, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'ada_usdt', 41, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'gram_usdt', 43, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'bnb_usdt', 45, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'wld_usdt', 47, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'mnt_usdt', 49, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'shib_usdt', 51, 'unsubscribe', 0, 0, NULL, 60), -- lbank
(9, 'bttc_usdt', 53, 'unsubscribe', 0, 0, NULL, 60); -- lbank
