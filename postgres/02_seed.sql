\c markets

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
(30, 'TMN'),
(31, 'IRR'),
(32, '1000PEPE'),
(33, '100PEPE'),
(34, 'TON');
SELECT setval(pg_get_serial_sequence('currencies', 'id'), (SELECT MAX(id) FROM currencies));

-- seed exchanges
INSERT INTO exchanges (id, name, label) VALUES
(1, 'nobitex', 'نوبیتکس'),
(2, 'bitpin', 'بیت پین'),
(3, 'wallex', 'والکس'),
(4, 'ramzinex', 'رمزینکس'),
(5, 'bitget', 'بیت گت'),
(6, 'bybit', 'بای بیت'),
(7, 'ompfinex', 'او ام پی فینکس'),
(8, 'okx', 'اوکی ایکس');
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
(81, 27, 30, 2, 8), -- BTT/TMN
(82, 1, 31, 2, 8), -- BTC/IRR
(83, 2, 31, 2, 8), -- ETH/IRR
(84, 3, 31, 2, 8), -- XRP/IRR
(85, 4, 31, 2, 8), -- TRX/IRR
(86, 5, 31, 2, 8), -- SOL/IRR
(87, 6, 31, 2, 8), -- DOT/IRR
(88, 7, 31, 2, 8), -- HYPE/IRR
(89, 8, 31, 2, 8), -- DOGE/IRR
(90, 32, 28, 2, 8), -- 1000PEPE/USDT
(91, 32, 31, 2, 8), -- 1000PEPE/IRR
(92, 10, 31, 2, 8), -- SUI/IRR
(93, 11, 31, 2, 8), -- ZEC/IRR
(94, 12, 31, 2, 8), -- XLM/IRR
(95, 13, 31, 2, 8), -- LINK/IRR
(96, 14, 31, 2, 8), -- AVAX/IRR
(97, 15, 31, 2, 8), -- PAXG/IRR
(98, 16, 31, 2, 8), -- XAUT/IRR
(99, 17, 31, 2, 8), -- NEAR/IRR
(100, 18, 31, 2, 8), -- UNI/IRR
(101, 19, 31, 2, 8), -- AAVE/IRR
(102, 21, 31, 2, 8), -- ADA/IRR
(103, 22, 31, 2, 8), -- GRAM/IRR
(104, 23, 31, 2, 8), -- BNB/IRR
(105, 24, 31, 2, 8), -- WLD/IRR
(106, 25, 31, 2, 8), -- MNT/IRR
(107, 26, 31, 2, 8), -- SHIB/IRR
(108, 27, 31, 2, 8), -- BTT/IRR
(109, 33, 28, 2, 8), -- 100PEPE/USDT
(110, 33, 31, 2, 8), -- 100PEPE/IRR
(111, 34, 28, 2, 8), -- TON/USDT
(112, 34, 29, 2, 8); -- TON/IRT
SELECT setval(pg_get_serial_sequence('markets', 'id'), (SELECT MAX(id) FROM markets));

-- seed exchange_markets
INSERT INTO exchange_markets (exchange_id, market, market_id, "status",last_action_token) VALUES
(1, 'BTCUSDT', 1, 'unsubscribe', NULL), -- nobitex BTC/USDT
(1, 'BTCIRT', 2, 'unsubscribe', NULL), -- nobitex BTC/IRT
(1, 'ETHUSDT', 3, 'unsubscribe', NULL), -- nobitex ETH/USDT
(1, 'ETHIRT', 4, 'unsubscribe', NULL), -- nobitex ETH/IRT
(1, 'XRPUSDT', 5, 'unsubscribe', NULL), -- nobitex XRP/USDT
(1, 'XRPIRT', 6, 'unsubscribe', NULL), -- nobitex XRP/IRT
(1, 'TRXUSDT', 7, 'unsubscribe', NULL), -- nobitex TRX/USDT
(1, 'TRXIRT', 8, 'unsubscribe', NULL), -- nobitex TRX/IRT
(1, 'SOLUSDT', 9, 'unsubscribe', NULL), -- nobitex SOL/USDT
(1, 'SOLIRT', 10, 'unsubscribe', NULL), -- nobitex SOL/IRT
(1, 'DOTUSDT', 11, 'unsubscribe', NULL), -- nobitex DOT/USDT
(1, 'DOTIRT', 12, 'unsubscribe', NULL), -- nobitex DOT/IRT
(1, 'HYPEUSDT', 13, 'unsubscribe', NULL), -- nobitex HYPE/USDT
(1, 'HYPEIRT', 14, 'unsubscribe', NULL), -- nobitex HYPE/IRT
(1, 'DOGEUSDT', 15, 'unsubscribe', NULL), -- nobitex DOGE/USDT
(1, 'DOGEIRT', 16, 'unsubscribe', NULL), -- nobitex DOGE/IRT
(1, 'PEPEUSDT', 17, 'unsubscribe', NULL), -- nobitex PEPE/USDT
(1, 'PEPEIRT', 18, 'unsubscribe', NULL), -- nobitex PEPE/IRT
(1, 'SUIUSDT', 19, 'unsubscribe', NULL), -- nobitex SUI/USDT
(1, 'SUIIRT', 20, 'unsubscribe', NULL), -- nobitex SUI/IRT
(1, 'ZECUSDT', 21, 'unsubscribe', NULL), -- nobitex ZEC/USDT
(1, 'ZECIRT', 22, 'unsubscribe', NULL), -- nobitex ZEC/IRT
(1, 'XLMUSDT', 23, 'unsubscribe', NULL), -- nobitex XLM/USDT
(1, 'XLMIRT', 24, 'unsubscribe', NULL), -- nobitex XLM/IRT
(1, 'LINKUSDT', 25, 'unsubscribe', NULL), -- nobitex LINK/USDT
(1, 'LINKIRT', 26, 'unsubscribe', NULL), -- nobitex LINK/IRT
(1, 'AVAXUSDT', 27, 'unsubscribe', NULL), -- nobitex AVAX/USDT
(1, 'AVAXIRT', 28, 'unsubscribe', NULL), -- nobitex AVAX/IRT
(1, 'PAXGUSDT', 29, 'unsubscribe', NULL), -- nobitex PAXG/USDT
(1, 'PAXGIRT', 30, 'unsubscribe', NULL), -- nobitex PAXG/IRT
(1, 'XAUTUSDT', 31, 'unsubscribe', NULL), -- nobitex XAUT/USDT
(1, 'XAUTIRT', 32, 'unsubscribe', NULL), -- nobitex XAUT/IRT
(1, 'NEARUSDT', 33, 'unsubscribe', NULL), -- nobitex NEAR/USDT
(1, 'NEARIRT', 34, 'unsubscribe', NULL), -- nobitex NEAR/IRT
(1, 'UNIUSDT', 35, 'unsubscribe', NULL), -- nobitex UNI/USDT
(1, 'UNIIRT', 36, 'unsubscribe', NULL), -- nobitex UNI/IRT
(1, 'AAVEUSDT', 37, 'unsubscribe', NULL), -- nobitex AAVE/USDT
(1, 'AAVEIRT', 38, 'unsubscribe', NULL), -- nobitex AAVE/IRT
(1, 'OKXUSDT', 39, 'unsubscribe', NULL), -- nobitex OKX/USDT
(1, 'OKXIRT', 40, 'unsubscribe', NULL), -- nobitex OKX/IRT
(1, 'ADAUSDT', 41, 'unsubscribe', NULL), -- nobitex ADA/USDT
(1, 'ADAIRT', 42, 'unsubscribe', NULL), -- nobitex ADA/IRT
(1, 'GRAMUSDT', 43, 'unsubscribe', NULL), -- nobitex GRAM/USDT
(1, 'GRAMIRT', 44, 'unsubscribe', NULL), -- nobitex GRAM/IRT
(1, 'BNBUSDT', 45, 'unsubscribe', NULL), -- nobitex BNB/USDT
(1, 'BNBIRT', 46, 'unsubscribe', NULL), -- nobitex BNB/IRT
(1, 'WLDUSDT', 47, 'unsubscribe', NULL), -- nobitex WLD/USDT
(1, 'WLDIRT', 48, 'unsubscribe', NULL), -- nobitex WLD/IRT
(1, 'MNTUSDT', 49, 'unsubscribe', NULL), -- nobitex MNT/USDT
(1, 'MNTIRT', 50, 'unsubscribe', NULL), -- nobitex MNT/IRT
(1, 'SHIBUSDT', 51, 'unsubscribe', NULL), -- nobitex SHIB/USDT
(1, 'SHIBIRT', 52, 'unsubscribe', NULL), -- nobitex SHIB/IRT
(1, 'BTTUSDT', 53, 'unsubscribe', NULL), -- nobitex BTT/USDT
(1, 'BTTIRT', 54, 'unsubscribe', NULL), -- nobitex BTT/IRT
(2, 'BTC_USDT', 1, 'unsubscribe', NULL), -- bitpin BTC/USDT
(2, 'BTC_IRT', 2, 'unsubscribe', NULL), -- bitpin BTC/IRT
(2, 'ETH_USDT', 3, 'unsubscribe', NULL), -- bitpin ETH/USDT
(2, 'ETH_IRT', 4, 'unsubscribe', NULL), -- bitpin ETH/IRT
(2, 'XRP_USDT', 5, 'unsubscribe', NULL), -- bitpin XRP/USDT
(2, 'XRP_IRT', 6, 'unsubscribe', NULL), -- bitpin XRP/IRT
(2, 'TRX_USDT', 7, 'unsubscribe', NULL), -- bitpin TRX/USDT
(2, 'TRX_IRT', 8, 'unsubscribe', NULL), -- bitpin TRX/IRT
(2, 'SOL_USDT', 9, 'unsubscribe', NULL), -- bitpin SOL/USDT
(2, 'SOL_IRT', 10, 'unsubscribe', NULL), -- bitpin SOL/IRT
(2, 'DOT_USDT', 11, 'unsubscribe', NULL), -- bitpin DOT/USDT
(2, 'DOT_IRT', 12, 'unsubscribe', NULL), -- bitpin DOT/IRT
(2, 'HYPE_USDT', 13, 'unsubscribe', NULL), -- bitpin HYPE/USDT
(2, 'HYPE_IRT', 14, 'unsubscribe', NULL), -- bitpin HYPE/IRT
(2, 'DOGE_USDT', 15, 'unsubscribe', NULL), -- bitpin DOGE/USDT
(2, 'DOGE_IRT', 16, 'unsubscribe', NULL), -- bitpin DOGE/IRT
(2, 'PEPE_USDT', 17, 'unsubscribe', NULL), -- bitpin PEPE/USDT
(2, 'PEPE_IRT', 18, 'unsubscribe', NULL), -- bitpin PEPE/IRT
(2, 'SUI_USDT', 19, 'unsubscribe', NULL), -- bitpin SUI/USDT
(2, 'SUI_IRT', 20, 'unsubscribe', NULL), -- bitpin SUI/IRT
(2, 'ZEC_USDT', 21, 'unsubscribe', NULL), -- bitpin ZEC/USDT
(2, 'ZEC_IRT', 22, 'unsubscribe', NULL), -- bitpin ZEC/IRT
(2, 'XLM_USDT', 23, 'unsubscribe', NULL), -- bitpin XLM/USDT
(2, 'XLM_IRT', 24, 'unsubscribe', NULL), -- bitpin XLM/IRT
(2, 'LINK_USDT', 25, 'unsubscribe', NULL), -- bitpin LINK/USDT
(2, 'LINK_IRT', 26, 'unsubscribe', NULL), -- bitpin LINK/IRT
(2, 'AVAX_USDT', 27, 'unsubscribe', NULL), -- bitpin AVAX/USDT
(2, 'AVAX_IRT', 28, 'unsubscribe', NULL), -- bitpin AVAX/IRT
(2, 'PAXG_USDT', 29, 'unsubscribe', NULL), -- bitpin PAXG/USDT
(2, 'PAXG_IRT', 30, 'unsubscribe', NULL), -- bitpin PAXG/IRT
(2, 'XAUT_USDT', 31, 'unsubscribe', NULL), -- bitpin XAUT/USDT
(2, 'XAUT_IRT', 32, 'unsubscribe', NULL), -- bitpin XAUT/IRT
(2, 'NEAR_USDT', 33, 'unsubscribe', NULL), -- bitpin NEAR/USDT
(2, 'NEAR_IRT', 34, 'unsubscribe', NULL), -- bitpin NEAR/IRT
(2, 'UNI_USDT', 35, 'unsubscribe', NULL), -- bitpin UNI/USDT
(2, 'UNI_IRT', 36, 'unsubscribe', NULL), -- bitpin UNI/IRT
(2, 'AAVE_USDT', 37, 'unsubscribe', NULL), -- bitpin AAVE/USDT
(2, 'AAVE_IRT', 38, 'unsubscribe', NULL), -- bitpin AAVE/IRT
(2, 'OKX_USDT', 39, 'unsubscribe', NULL), -- bitpin OKX/USDT
(2, 'OKX_IRT', 40, 'unsubscribe', NULL), -- bitpin OKX/IRT
(2, 'ADA_USDT', 41, 'unsubscribe', NULL), -- bitpin ADA/USDT
(2, 'ADA_IRT', 42, 'unsubscribe', NULL), -- bitpin ADA/IRT
(2, 'GRAM_USDT', 43, 'unsubscribe', NULL), -- bitpin GRAM/USDT
(2, 'GRAM_IRT', 44, 'unsubscribe', NULL), -- bitpin GRAM/IRT
(2, 'BNB_USDT', 45, 'unsubscribe', NULL), -- bitpin BNB/USDT
(2, 'BNB_IRT', 46, 'unsubscribe', NULL), -- bitpin BNB/IRT
(2, 'WLD_USDT', 47, 'unsubscribe', NULL), -- bitpin WLD/USDT
(2, 'WLD_IRT', 48, 'unsubscribe', NULL), -- bitpin WLD/IRT
(2, 'MNT_USDT', 49, 'unsubscribe', NULL), -- bitpin MNT/USDT
(2, 'MNT_IRT', 50, 'unsubscribe', NULL), -- bitpin MNT/IRT
(2, 'SHIB_USDT', 51, 'unsubscribe', NULL), -- bitpin SHIB/USDT
(2, 'SHIB_IRT', 52, 'unsubscribe', NULL), -- bitpin SHIB/IRT
(2, 'BTT_USDT', 53, 'unsubscribe', NULL), -- bitpin BTT/USDT
(2, 'BTT_IRT', 54, 'unsubscribe', NULL), -- bitpin BTT/IRT
(3, 'BTCUSDT', 1, 'unsubscribe', NULL), -- wallex BTC/USDT
(3, 'BTCTMN', 55, 'unsubscribe', NULL), -- wallex BTC/TMN
(3, 'ETHUSDT', 3, 'unsubscribe', NULL), -- wallex ETH/USDT
(3, 'ETHTMN', 56, 'unsubscribe', NULL), -- wallex ETH/TMN
(3, 'XRPUSDT', 5, 'unsubscribe', NULL), -- wallex XRP/USDT
(3, 'XRPTMN', 57, 'unsubscribe', NULL), -- wallex XRP/TMN
(3, 'TRXUSDT', 7, 'unsubscribe', NULL), -- wallex TRX/USDT
(3, 'TRXTMN', 58, 'unsubscribe', NULL), -- wallex TRX/TMN
(3, 'SOLUSDT', 9, 'unsubscribe', NULL), -- wallex SOL/USDT
(3, 'SOLTMN', 59, 'unsubscribe', NULL), -- wallex SOL/TMN
(3, 'DOTUSDT', 11, 'unsubscribe', NULL), -- wallex DOT/USDT
(3, 'DOTTMN', 60, 'unsubscribe', NULL), -- wallex DOT/TMN
(3, 'HYPEUSDT', 13, 'unsubscribe', NULL), -- wallex HYPE/USDT
(3, 'HYPETMN', 61, 'unsubscribe', NULL), -- wallex HYPE/TMN
(3, 'DOGEUSDT', 15, 'unsubscribe', NULL), -- wallex DOGE/USDT
(3, 'DOGETMN', 62, 'unsubscribe', NULL), -- wallex DOGE/TMN
(3, 'PEPEUSDT', 17, 'unsubscribe', NULL), -- wallex PEPE/USDT
(3, 'PEPETMN', 63, 'unsubscribe', NULL), -- wallex PEPE/TMN
(3, 'SUIUSDT', 19, 'unsubscribe', NULL), -- wallex SUI/USDT
(3, 'SUITMN', 64, 'unsubscribe', NULL), -- wallex SUI/TMN
(3, 'ZECUSDT', 21, 'unsubscribe', NULL), -- wallex ZEC/USDT
(3, 'ZECTMN', 65, 'unsubscribe', NULL), -- wallex ZEC/TMN
(3, 'XLMUSDT', 23, 'unsubscribe', NULL), -- wallex XLM/USDT
(3, 'XLMTMN', 66, 'unsubscribe', NULL), -- wallex XLM/TMN
(3, 'LINKUSDT', 25, 'unsubscribe', NULL), -- wallex LINK/USDT
(3, 'LINKTMN', 67, 'unsubscribe', NULL), -- wallex LINK/TMN
(3, 'AVAXUSDT', 27, 'unsubscribe', NULL), -- wallex AVAX/USDT
(3, 'AVAXTMN', 68, 'unsubscribe', NULL), -- wallex AVAX/TMN
(3, 'PAXGUSDT', 29, 'unsubscribe', NULL), -- wallex PAXG/USDT
(3, 'PAXGTMN', 69, 'unsubscribe', NULL), -- wallex PAXG/TMN
(3, 'XAUTUSDT', 31, 'unsubscribe', NULL), -- wallex XAUT/USDT
(3, 'XAUTTMN', 70, 'unsubscribe', NULL), -- wallex XAUT/TMN
(3, 'NEARUSDT', 33, 'unsubscribe', NULL), -- wallex NEAR/USDT
(3, 'NEARTMN', 71, 'unsubscribe', NULL), -- wallex NEAR/TMN
(3, 'UNIUSDT', 35, 'unsubscribe', NULL), -- wallex UNI/USDT
(3, 'UNITMN', 72, 'unsubscribe', NULL), -- wallex UNI/TMN
(3, 'AAVEUSDT', 37, 'unsubscribe', NULL), -- wallex AAVE/USDT
(3, 'AAVETMN', 73, 'unsubscribe', NULL), -- wallex AAVE/TMN
(3, 'OKXUSDT', 39, 'unsubscribe', NULL), -- wallex OKX/USDT
(3, 'OKXTMN', 74, 'unsubscribe', NULL), -- wallex OKX/TMN
(3, 'ADAUSDT', 41, 'unsubscribe', NULL), -- wallex ADA/USDT
(3, 'ADATMN', 75, 'unsubscribe', NULL), -- wallex ADA/TMN
(3, 'GRAMUSDT', 43, 'unsubscribe', NULL), -- wallex GRAM/USDT
(3, 'GRAMTMN', 76, 'unsubscribe', NULL), -- wallex GRAM/TMN
(3, 'BNBUSDT', 45, 'unsubscribe', NULL), -- wallex BNB/USDT
(3, 'BNBTMN', 77, 'unsubscribe', NULL), -- wallex BNB/TMN
(3, 'WLDUSDT', 47, 'unsubscribe', NULL), -- wallex WLD/USDT
(3, 'WLDTMN', 78, 'unsubscribe', NULL), -- wallex WLD/TMN
(3, 'MNTUSDT', 49, 'unsubscribe', NULL), -- wallex MNT/USDT
(3, 'MNTTMN', 79, 'unsubscribe', NULL), -- wallex MNT/TMN
(3, 'SHIBUSDT', 51, 'unsubscribe', NULL), -- wallex SHIB/USDT
(3, 'SHIBTMN', 80, 'unsubscribe', NULL), -- wallex SHIB/TMN
(3, 'BTTUSDT', 53, 'unsubscribe', NULL), -- wallex BTT/USDT
(3, 'BTTTMN', 81, 'unsubscribe', NULL), -- wallex BTT/TMN
(4, '12', 1, 'unsubscribe', NULL), -- ramzinex BTC/USDT
(4, '2', 82, 'unsubscribe', NULL), -- ramzinex BTC/IRR
(4, '13', 3, 'unsubscribe', NULL), -- ramzinex ETH/USDT
(4, '3', 83, 'unsubscribe', NULL), -- ramzinex ETH/IRR
(4, '643', 5, 'unsubscribe', NULL), -- ramzinex XRP/USDT
(4, '4', 84, 'unsubscribe', NULL), -- ramzinex XRP/IRR
(4, '27', 7, 'unsubscribe', NULL), -- ramzinex TRX/USDT
(4, '25', 85, 'unsubscribe', NULL), -- ramzinex TRX/IRR
(4, '218', 9, 'unsubscribe', NULL), -- ramzinex SOL/USDT
(4, '96', 86, 'unsubscribe', NULL), -- ramzinex SOL/IRR
(4, '31', 11, 'unsubscribe', NULL), -- ramzinex DOT/USDT
(4, '29', 87, 'unsubscribe', NULL), -- ramzinex DOT/IRR
(4, '732', 13, 'unsubscribe', NULL), -- ramzinex HYPE/USDT
(4, '731', 88, 'unsubscribe', NULL), -- ramzinex HYPE/IRR
(4, '432', 15, 'unsubscribe', NULL), -- ramzinex DOGE/USDT
(4, '10', 89, 'unsubscribe', NULL), -- ramzinex DOGE/IRR
(4, '554', 90, 'unsubscribe', NULL), -- ramzinex 1000PEPE/USDT
(4, '397', 91, 'unsubscribe', NULL), -- ramzinex 1000PEPE/IRR
(4, '700', 19, 'unsubscribe', NULL), -- ramzinex SUI/USDT
(4, '699', 92, 'unsubscribe', NULL), -- ramzinex SUI/IRR
(4, '451', 21, 'unsubscribe', NULL), -- ramzinex ZEC/USDT
(4, '291', 93, 'unsubscribe', NULL), -- ramzinex ZEC/IRR
(4, '19', 94, 'unsubscribe', NULL), -- ramzinex XLM/IRR
(4, '38', 25, 'unsubscribe', NULL), -- ramzinex LINK/USDT
(4, '26', 95, 'unsubscribe', NULL), -- ramzinex LINK/IRR
(4, '741', 27, 'unsubscribe', NULL), -- ramzinex AVAX/USDT
(4, '255', 96, 'unsubscribe', NULL), -- ramzinex AVAX/IRR
(4, '881', 29, 'unsubscribe', NULL), -- ramzinex PAXG/USDT
(4, '296', 97, 'unsubscribe', NULL), -- ramzinex PAXG/IRR
(4, '899', 31, 'unsubscribe', NULL), -- ramzinex XAUT/USDT
(4, '896', 98, 'unsubscribe', NULL), -- ramzinex XAUT/IRR
(4, '267', 99, 'unsubscribe', NULL), -- ramzinex NEAR/IRR
(4, '35', 35, 'unsubscribe', NULL), -- ramzinex UNI/USDT
(4, '34', 100, 'unsubscribe', NULL), -- ramzinex UNI/IRR
(4, '37', 37, 'unsubscribe', NULL), -- ramzinex AAVE/USDT
(4, '36', 101, 'unsubscribe', NULL), -- ramzinex AAVE/IRR
(4, '158', 41, 'unsubscribe', NULL), -- ramzinex ADA/USDT
(4, '33', 102, 'unsubscribe', NULL), -- ramzinex ADA/IRR
(4, '434', 43, 'unsubscribe', NULL), -- ramzinex GRAM/USDT
(4, '272', 103, 'unsubscribe', NULL), -- ramzinex GRAM/IRR
(4, '18', 45, 'unsubscribe', NULL), -- ramzinex BNB/USDT
(4, '17', 104, 'unsubscribe', NULL), -- ramzinex BNB/IRR
(4, '642', 47, 'unsubscribe', NULL), -- ramzinex WLD/USDT
(4, '376', 105, 'unsubscribe', NULL), -- ramzinex WLD/IRR
(4, '344', 106, 'unsubscribe', NULL), -- ramzinex MNT/IRR
(4, '518', 51, 'unsubscribe', NULL), -- ramzinex SHIB/USDT
(4, '61', 107, 'unsubscribe', NULL), -- ramzinex SHIB/IRR
(4, '39', 108, 'unsubscribe', NULL), -- ramzinex BTT/IRR
(4, '552', 109, 'unsubscribe', NULL), -- ramzinex 100PEPE/USDT
(4, '366', 110, 'unsubscribe', NULL), -- ramzinex 100PEPE/IRR
(5, 'BTCUSDT', 1, 'unsubscribe', NULL), -- bitget BTC/USDT
(5, 'ETHUSDT', 3, 'unsubscribe', NULL), -- bitget ETH/USDT
(5, 'XRPUSDT', 5, 'unsubscribe', NULL), -- bitget XRP/USDT
(5, 'TRXUSDT', 7, 'unsubscribe', NULL), -- bitget TRX/USDT
(5, 'SOLUSDT', 9, 'unsubscribe', NULL), -- bitget SOL/USDT
(5, 'DOTUSDT', 11, 'unsubscribe', NULL), -- bitget DOT/USDT
(5, 'HYPEUSDT', 13, 'unsubscribe', NULL), -- bitget HYPE/USDT
(5, 'DOGEUSDT', 15, 'unsubscribe', NULL), -- bitget DOGE/USDT
(5, 'PEPEUSDT', 17, 'unsubscribe', NULL), -- bitget PEPE/USDT
(5, 'SUIUSDT', 19, 'unsubscribe', NULL), -- bitget SUI/USDT
(5, 'ZECUSDT', 21, 'unsubscribe', NULL), -- bitget ZEC/USDT
(5, 'XLMUSDT', 23, 'unsubscribe', NULL), -- bitget XLM/USDT
(5, 'LINKUSDT', 25, 'unsubscribe', NULL), -- bitget LINK/USDT
(5, 'AVAXUSDT', 27, 'unsubscribe', NULL), -- bitget AVAX/USDT
(5, 'PAXGUSDT', 29, 'unsubscribe', NULL), -- bitget PAXG/USDT
(5, 'XAUTUSDT', 31, 'unsubscribe', NULL), -- bitget XAUT/USDT
(5, 'NEARUSDT', 33, 'unsubscribe', NULL), -- bitget NEAR/USDT
(5, 'UNIUSDT', 35, 'unsubscribe', NULL), -- bitget UNI/USDT
(5, 'AAVEUSDT', 37, 'unsubscribe', NULL), -- bitget AAVE/USDT
(5, 'OKXUSDT', 39, 'unsubscribe', NULL), -- bitget OKX/USDT
(5, 'ADAUSDT', 41, 'unsubscribe', NULL), -- bitget ADA/USDT
(5, 'GRAMUSDT', 43, 'unsubscribe', NULL), -- bitget GRAM/USDT
(5, 'BNBUSDT', 45, 'unsubscribe', NULL), -- bitget BNB/USDT
(5, 'WLDUSDT', 47, 'unsubscribe', NULL), -- bitget WLD/USDT
(5, 'MNTUSDT', 49, 'unsubscribe', NULL), -- bitget MNT/USDT
(5, 'SHIBUSDT', 51, 'unsubscribe', NULL), -- bitget SHIB/USDT
(5, 'BTTUSDT', 53, 'unsubscribe', NULL), -- bitget BTT/USDT
(6, 'BTCUSDT', 1, 'unsubscribe', NULL), -- bybit BTC/USDT
(6, 'ETHUSDT', 3, 'unsubscribe', NULL), -- bybit ETH/USDT
(6, 'XRPUSDT', 5, 'unsubscribe', NULL), -- bybit XRP/USDT
(6, 'TRXUSDT', 7, 'unsubscribe', NULL), -- bybit TRX/USDT
(6, 'SOLUSDT', 9, 'unsubscribe', NULL), -- bybit SOL/USDT
(6, 'DOTUSDT', 11, 'unsubscribe', NULL), -- bybit DOT/USDT
(6, 'HYPEUSDT', 13, 'unsubscribe', NULL), -- bybit HYPE/USDT
(6, 'DOGEUSDT', 15, 'unsubscribe', NULL), -- bybit DOGE/USDT
(6, 'PEPEUSDT', 17, 'unsubscribe', NULL), -- bybit PEPE/USDT
(6, 'SUIUSDT', 19, 'unsubscribe', NULL), -- bybit SUI/USDT
(6, 'ZECUSDT', 21, 'unsubscribe', NULL), -- bybit ZEC/USDT
(6, 'XLMUSDT', 23, 'unsubscribe', NULL), -- bybit XLM/USDT
(6, 'LINKUSDT', 25, 'unsubscribe', NULL), -- bybit LINK/USDT
(6, 'AVAXUSDT', 27, 'unsubscribe', NULL), -- bybit AVAX/USDT
(6, 'PAXGUSDT', 29, 'unsubscribe', NULL), -- bybit PAXG/USDT
(6, 'XAUTUSDT', 31, 'unsubscribe', NULL), -- bybit XAUT/USDT
(6, 'NEARUSDT', 33, 'unsubscribe', NULL), -- bybit NEAR/USDT
(6, 'UNIUSDT', 35, 'unsubscribe', NULL), -- bybit UNI/USDT
(6, 'AAVEUSDT', 37, 'unsubscribe', NULL), -- bybit AAVE/USDT
(6, 'OKXUSDT', 39, 'unsubscribe', NULL), -- bybit OKX/USDT
(6, 'ADAUSDT', 41, 'unsubscribe', NULL), -- bybit ADA/USDT
(6, 'GRAMUSDT', 43, 'unsubscribe', NULL), -- bybit GRAM/USDT
(6, 'BNBUSDT', 45, 'unsubscribe', NULL), -- bybit BNB/USDT
(6, 'WLDUSDT', 47, 'unsubscribe', NULL), -- bybit WLD/USDT
(6, 'MNTUSDT', 49, 'unsubscribe', NULL), -- bybit MNT/USDT
(6, 'SHIBUSDT', 51, 'unsubscribe', NULL), -- bybit SHIB/USDT
(6, 'BTTUSDT', 53, 'unsubscribe', NULL), -- bybit BTT/USDT
(7, '14', 1, 'unsubscribe', NULL), -- ompfinex BTC/USDT
(7, '1', 2, 'unsubscribe', NULL), -- ompfinex BTC/IRT
(7, '15', 3, 'unsubscribe', NULL), -- ompfinex ETH/USDT
(7, '2', 4, 'unsubscribe', NULL), -- ompfinex ETH/IRT
(7, '17', 5, 'unsubscribe', NULL), -- ompfinex XRP/USDT
(7, '4', 6, 'unsubscribe', NULL), -- ompfinex XRP/IRT
(7, '21', 7, 'unsubscribe', NULL), -- ompfinex TRX/USDT
(7, '8', 8, 'unsubscribe', NULL), -- ompfinex TRX/IRT
(7, '28', 9, 'unsubscribe', NULL), -- ompfinex SOL/USDT
(7, '27', 10, 'unsubscribe', NULL), -- ompfinex SOL/IRT
(7, '63', 11, 'unsubscribe', NULL), -- ompfinex DOT/USDT
(7, '62', 12, 'unsubscribe', NULL), -- ompfinex DOT/IRT
(7, '357', 13, 'unsubscribe', NULL), -- ompfinex HYPE/USDT
(7, '358', 14, 'unsubscribe', NULL), -- ompfinex HYPE/IRT
(7, '18', 15, 'unsubscribe', NULL), -- ompfinex DOGE/USDT
(7, '5', 16, 'unsubscribe', NULL), -- ompfinex DOGE/IRT
(7, '168', 17, 'unsubscribe', NULL), -- ompfinex PEPE/USDT
(7, '167', 18, 'unsubscribe', NULL), -- ompfinex PEPE/IRT
(7, '295', 19, 'unsubscribe', NULL), -- ompfinex SUI/USDT
(7, '296', 20, 'unsubscribe', NULL), -- ompfinex SUI/IRT
(7, '23', 23, 'unsubscribe', NULL), -- ompfinex XLM/USDT
(7, '11', 24, 'unsubscribe', NULL), -- ompfinex XLM/IRT
(7, '77', 25, 'unsubscribe', NULL), -- ompfinex LINK/USDT
(7, '76', 26, 'unsubscribe', NULL), -- ompfinex LINK/IRT
(7, '156', 27, 'unsubscribe', NULL), -- ompfinex AVAX/USDT
(7, '155', 28, 'unsubscribe', NULL), -- ompfinex AVAX/IRT
(7, '134', 29, 'unsubscribe', NULL), -- ompfinex PAXG/USDT
(7, '133', 30, 'unsubscribe', NULL), -- ompfinex PAXG/IRT
(7, '136', 33, 'unsubscribe', NULL), -- ompfinex NEAR/USDT
(7, '135', 34, 'unsubscribe', NULL), -- ompfinex NEAR/IRT
(7, '112', 35, 'unsubscribe', NULL), -- ompfinex UNI/USDT
(7, '111', 36, 'unsubscribe', NULL), -- ompfinex UNI/IRT
(7, '71', 37, 'unsubscribe', NULL), -- ompfinex AAVE/USDT
(7, '70', 38, 'unsubscribe', NULL), -- ompfinex AAVE/IRT
(7, '22', 41, 'unsubscribe', NULL), -- ompfinex ADA/USDT
(7, '10', 42, 'unsubscribe', NULL), -- ompfinex ADA/IRT
(7, '19', 45, 'unsubscribe', NULL), -- ompfinex BNB/USDT
(7, '6', 46, 'unsubscribe', NULL), -- ompfinex BNB/IRT
(7, '266', 47, 'unsubscribe', NULL), -- ompfinex WLD/USDT
(7, '267', 48, 'unsubscribe', NULL), -- ompfinex WLD/IRT
(7, '403', 49, 'unsubscribe', NULL), -- ompfinex MNT/USDT
(7, '404', 50, 'unsubscribe', NULL), -- ompfinex MNT/IRT
(7, '31', 51, 'unsubscribe', NULL), -- ompfinex SHIB/USDT
(7, '30', 52, 'unsubscribe', NULL), -- ompfinex SHIB/IRT
(7, '196', 53, 'unsubscribe', NULL), -- ompfinex BTT/USDT
(7, '195', 54, 'unsubscribe', NULL), -- ompfinex BTT/IRT
(7, '142', 111, 'unsubscribe', NULL), -- ompfinex TON/USDT
(7, '141', 112, 'unsubscribe', NULL), -- ompfinex TON/IRT
(8, 'BTC-USDT', 1, 'unsubscribe', NULL), -- okx BTC/USDT
(8, 'ETH-USDT', 3, 'unsubscribe', NULL), -- okx ETH/USDT
(8, 'XRP-USDT', 5, 'unsubscribe', NULL), -- okx XRP/USDT
(8, 'TRX-USDT', 7, 'unsubscribe', NULL), -- okx TRX/USDT
(8, 'SOL-USDT', 9, 'unsubscribe', NULL), -- okx SOL/USDT
(8, 'DOT-USDT', 11, 'unsubscribe', NULL), -- okx DOT/USDT
(8, 'HYPE-USDT', 13, 'unsubscribe', NULL), -- okx HYPE/USDT
(8, 'DOGE-USDT', 15, 'unsubscribe', NULL), -- okx DOGE/USDT
(8, 'PEPE-USDT', 17, 'unsubscribe', NULL), -- okx PEPE/USDT
(8, 'SUI-USDT', 19, 'unsubscribe', NULL), -- okx SUI/USDT
(8, 'ZEC-USDT', 21, 'unsubscribe', NULL), -- okx ZEC/USDT
(8, 'XLM-USDT', 23, 'unsubscribe', NULL), -- okx XLM/USDT
(8, 'LINK-USDT', 25, 'unsubscribe', NULL), -- okx LINK/USDT
(8, 'AVAX-USDT', 27, 'unsubscribe', NULL), -- okx AVAX/USDT
(8, 'PAXG-USDT', 29, 'unsubscribe', NULL), -- okx PAXG/USDT
(8, 'XAUT-USDT', 31, 'unsubscribe', NULL), -- okx XAUT/USDT
(8, 'NEAR-USDT', 33, 'unsubscribe', NULL), -- okx NEAR/USDT
(8, 'UNI-USDT', 35, 'unsubscribe', NULL), -- okx UNI/USDT
(8, 'AAVE-USDT', 37, 'unsubscribe', NULL), -- okx AAVE/USDT
(8, 'OKX-USDT', 39, 'unsubscribe', NULL), -- okx OKX/USDT
(8, 'ADA-USDT', 41, 'unsubscribe', NULL), -- okx ADA/USDT
(8, 'GRAM-USDT', 43, 'unsubscribe', NULL), -- okx GRAM/USDT
(8, 'BNB-USDT', 45, 'unsubscribe', NULL), -- okx BNB/USDT
(8, 'WLD-USDT', 47, 'unsubscribe', NULL), -- okx WLD/USDT
(8, 'MNT-USDT', 49, 'unsubscribe', NULL), -- okx MNT/USDT
(8, 'SHIB-USDT', 51, 'unsubscribe', NULL), -- okx SHIB/USDT
(8, 'BTT-USDT', 53, 'unsubscribe', NULL); -- okx BTT/USDT
