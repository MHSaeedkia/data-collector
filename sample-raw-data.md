# Raw Topic Sample Data (`ex{id}-raw`)

Captured 2026-07-13 from kafka-ui at `http://192.168.150.104:8080/` (cluster `local`), latest 200
messages per topic via the messages API. Every record: **key = null**, single partition, value =
verbatim exchange payload as forwarded by NiFi (no envelope added by NiFi).

Exchange id ↔ name mapping confirmed from the server's `exchanges` table; the market identifier
embedded in each payload's channel/topic string is exactly `exchange_markets.market` for that
exchange (verified against the server's subscribed rows).

| Topic     | Exchange | Style                                                 | Market key in payload                              | Sequence fields                 | Price/qty wire type                  |
| --------- | -------- | ----------------------------------------------------- | -------------------------------------------------- | ------------------------------- | ------------------------------------ |
| `ex1-raw` | nobitex  | full book (24×24) every msg                           | channel `public:orderbook-{MARKET}`                | `pub.offset`, `lastUpdate` (ms) | strings                              |
| `ex2-raw` | bitpin   | full book (50×50) every msg                           | channel `orderbook:{MARKET}`                       | `pub.offset`, `event_time`      | strings (but `price` field = number) |
| `ex3-raw` | wallex   | full book, **one side per msg** (≤50)                 | event name `{MARKET}@buyDepth\|sellDepth`          | **none**                        | **numbers**                          |
| `ex4-raw` | ramzinex | full book (50×50) every msg                           | channel `orderbook:{MARKET}` (numeric)             | `pub.offset`, per-row ts (ms)   | **numbers**                          |
| `ex5-raw` | bitget   | `action: snapshot\|update` (only `snapshot` observed) | `arg.instId`                                       | `seq`, `pseq`, `ts`             | strings                              |
| `ex6-raw` | bybit    | `type: snapshot\|delta` (only `delta` observed)       | topic `orderbook.50.{MARKET}`, `data.s`            | `u`, `seq`, `ts`, `cts`         | strings                              |
| `ex7-raw` | ompfinex | delta only (Binance-style diff)                       | channel `public-market:r-depth-{MARKET}` (numeric) | `U`/`u` range, `pub.offset`     | strings                              |

The server DB also has exchange **8 = okx**, but no `ex8-raw` topic exists yet.

`ex1`, `ex2`, `ex4`, `ex7` are wrapped in a Centrifugo push envelope:
`{"push": {"channel": ..., "pub": {"data": <payload>, "offset": <int>}}}` — `pub.offset` is the
per-channel Centrifugo publication counter.

---

## ex1-raw — nobitex

- Channel `public:orderbook-{MARKET}` (e.g. `public:orderbook-SOLIRT`).
- `data`: `asks`/`bids` = arrays of `["price","quantity"]` **strings**; always 24 levels per side
  in the sample (top-24 book, full replacement each message); plus `lastTradePrice` (string),
  `lastUpdate` (epoch ms).
- No update/delta variant observed — every message is a full snapshot.

Verbatim record (offset 27437, 2026-07-13T08:25:02.707Z):

```json
{
	"push": {
		"channel": "public:orderbook-SOLUSDT",
		"pub": {
			"data": {
				"asks": [
					["76.711", "1"],
					["76.733", "2.298"],
					["76.748", "70.5"],
					["76.76", "0.958"],
					["76.765", "1.693"],
					["76.8", "1.612"],
					["77.134", "0.262"],
					["77.135", "38.834"],
					["77.2", "3.846"],
					["77.3", "2.011"],
					["77.38", "1.084"],
					["77.4", "2.302"],
					["77.406", "0.129"],
					["77.499", "1.314"],
					["77.6", "3.997"],
					["77.8", "17.849"],
					["77.9", "95.94"],
					["78", "6.156"],
					["78.054", "0.128"],
					["78.23", "0.27"],
					["78.453", "1.861"],
					["78.49", "10"],
					["78.499", "1.273"],
					["78.5", "0.708"]
				],
				"bids": [
					["76.55", "0.6"],
					["76.477", "1"],
					["76.445", "1"],
					["76.444", "6.63"],
					["76.403", "0.163"],
					["76.4", "32.107"],
					["76.35", "3.145"],
					["76.218", "18.7"],
					["76.217", "0.076"],
					["76.2", "2.312"],
					["76.15", "5"],
					["76.148", "9.191"],
					["76.121", "6.568"],
					["76.117", "38.834"],
					["76", "14.497"],
					["75.9", "1.317"],
					["75.82", "0.202"],
					["75.6", "0.251"],
					["75.58", "0.568"],
					["75.5", "4.536"],
					["75.2", "1.329"],
					["75.195", "0.083"],
					["75.18", "1"],
					["75.1", "1"]
				],
				"lastTradePrice": "76.55",
				"lastUpdate": 1783931102472
			},
			"offset": 1204296
		}
	}
}
```

### ⚠ Multi-document records

3 of 200 records contained **two newline-concatenated JSON documents in one Kafka record** — and
they can belong to **different channels**. Job 1 must split records into consecutive JSON
documents, not assume one document per record.

Verbatim record (offset 27353, 2026-07-13T08:24:53.656Z):

```json
{"push":{"channel":"public:orderbook-XRPIRT","pub":{"data":{"asks":[["1942090","494.4"],["1942130","43.5"],["1947470","8.8"],["1947480","50"],["1947500","22.6"],["1949420","647.4"],["1950000","1099.7"],["1950500","20"],["1954900","14"],["1954950","29.5"],["1960000","90"],["1960070","0.3"],["1960080","570.8"],["1960100","702.1"],["1960990","505.3"],["1961000","961.3"],["1961110","505.2"],["1961200","761.1"],["1964770","0.3"],["1965000","199.2"],["1969630","0.3"],["1969970","167.2"],["1969990","101.3"],["1970000","523.2"]],"bids":[["1939850","11.3"],["1939840","0.6"],["1939830","37.8"],["1938170","88.4"],["1938110","9.9"],["1937270","50"],["1937250","0.3"],["1934970","9.8"],["1934960","5.1"],["1934940","100"],["1932960","100.3"],["1932000","1"],["1931000","18.4"],["1930710","12"],["1930020","380.2"],["1930000","2040.6"],["1929970","207.2"],["1929960","2520.9"],["1928000","100.3"],["1927430","648.3"],["1926000","255.4"],["1925000","5.3"],["1924130","0.3"],["1921710","152.7"]],"lastTradePrice":"1939840","lastUpdate":1783931093471},"offset":1079308}}}
{"push":{"channel":"public:orderbook-SOLIRT","pub":{"data":{"asks":[["138132630","1"],["138132640","0.496"],["138132700","2.609"],["138132860","0.324"],["138137260","13.119"],["138137270","0.265"],["138151720","0.004"],["138200000","0.228"],["138500000","2.456"],["138557190","0.004"],["138997750","3"],["139000000","62.574"],["139055000","0.004"],["139400060","0.88"],["139460000","1.417"],["139490000","2.229"],["139500000","5.069"],["139779740","0.004"],["139900000","0.019"],["140000000","48.205"],["140170000","0.1"],["140180000","0.1"],["140267200","0.004"],["140295410","2.888"]],"bids":[["137129740","1.035"],["137129730","1.218"],["137096850","0.232"],["136939020","0.021"],["136939010","3.358"],["136543940","0.004"],["136300000","0.512"],["136230460","9.167"],["136200000","0.59"],["136100000","5"],["136084460","0.023"],["136047030","0.004"],["136000050","0.011"],["136000000","18.344"],["135952550","0.004"],["135950000","0.073"],["135762680","0.072"],["135648440","0.004"],["135531050","0.011"],["135500000","0.983"],["135400000","0.078"],["135373750","0.09"],["135300000","0.1"],["135240500","0.004"]],"lastTradePrice":"136950170","lastUpdate":1783931093466},"offset":1399335}}}
```

---

## ex2-raw — bitpin

- Channel `orderbook:{MARKET}` with `MARKET` like `BTC_IRT` (underscore form = `exchange_markets.market`).
- `data`: `asks`/`bids` = arrays of `["price","quantity"]` **strings**, 50 levels per side (bids
  occasionally fewer); `event` = always `"market_data"` (200/200); `symbol`, `event_time`
  (ISO-ish string), `price` (last price, **JSON number**), `volume_ask`/`volume_bid` (strings).
- Full snapshot every message; no delta variant observed.

Verbatim record (offset 55173, 2026-07-13T08:25:22.849Z):

```json
{
	"push": {
		"channel": "orderbook:TRX_USDT",
		"pub": {
			"data": {
				"bids": [
					["0.3299", "549.62"],
					["0.3291", "91.19"],
					["0.3288", "60.59"],
					["0.3284", "10153.80"],
					["0.3283", "10163.06"],
					["0.3282", "21760.53"],
					["0.3281", "22004.83"],
					["0.3280", "10849.73"],
					["0.3279", "46520.09"],
					["0.3278", "21624.33"],
					["0.3275", "139870.95"],
					["0.3274", "103699.44"],
					["0.3273", "35927.03"],
					["0.3272", "35032.88"],
					["0.3271", "71716.78"],
					["0.3270", "140714.71"],
					["0.3269", "102152.19"],
					["0.3268", "35530.07"],
					["0.3260", "90.52"],
					["0.3255", "122.94"],
					["0.3250", "3330.38"],
					["0.3248", "121.95"],
					["0.3205", "114.29"],
					["0.3194", "2097.68"],
					["0.3151", "93.37"],
					["0.3150", "31.74"],
					["0.3100", "32.25"],
					["0.3000", "1436.54"],
					["0.2907", "3.54"],
					["0.2687", "12.44"],
					["0.2684", "186.28"],
					["0.2488", "60.60"],
					["0.2345", "4.26"],
					["0.2008", "2598.78"],
					["0.2000", "108.50"],
					["0.1808", "303.03"],
					["0.1592", "0.50"],
					["0.1555", "12.86"],
					["0.1408", "121.21"],
					["0.1208", "60.60"],
					["0.0664", "1.80"]
				],
				"asks": [
					["0.3311", "18448.94"],
					["0.3312", "20698.13"],
					["0.3313", "11569.64"],
					["0.3314", "10857.83"],
					["0.3315", "21765.34"],
					["0.3316", "22356.02"],
					["0.3319", "35689.82"],
					["0.3320", "175566.57"],
					["0.3321", "210971.99"],
					["0.3322", "140870.11"],
					["0.3323", "35654.31"],
					["0.3324", "33817.10"],
					["0.3326", "35500.50"],
					["0.3327", "35550.78"],
					["0.3330", "648.73"],
					["0.3335", "830.65"],
					["0.3336", "1238.82"],
					["0.3340", "362.79"],
					["0.3344", "151.42"],
					["0.3348", "694.14"],
					["0.3350", "98.88"],
					["0.3360", "33.94"],
					["0.3362", "30.06"],
					["0.3364", "531.26"],
					["0.3365", "390.71"],
					["0.3371", "247.77"],
					["0.3384", "3117.03"],
					["0.3385", "4264.41"],
					["0.3390", "329.96"],
					["0.3396", "60.04"],
					["0.3399", "108.74"],
					["0.3400", "4289.62"],
					["0.3420", "885.01"],
					["0.3440", "84.21"],
					["0.3450", "642.70"],
					["0.3453", "548.33"],
					["0.3454", "526.42"],
					["0.3456", "3.79"],
					["0.3460", "93.12"],
					["0.3468", "29.89"],
					["0.3500", "21489.53"],
					["0.3533", "272.45"],
					["0.3558", "478.08"],
					["0.3560", "378.66"],
					["0.3590", "241.38"],
					["0.3600", "1213.61"],
					["0.3604", "5435.54"],
					["0.3612", "360.35"],
					["0.3619", "120.23"],
					["0.3650", "1555.11"]
				],
				"volume_ask": "861104.46",
				"volume_bid": "819367.88",
				"symbol": "TRX_USDT",
				"event": "market_data",
				"price": 0.3311,
				"event_time": "2026-07-13T08:25:22.699081Z"
			},
			"offset": 7955331
		}
	}
}
```

---

## ex3-raw — wallex

- Socket.io-style array: `["{MARKET}@buyDepth", [levels...]]` — **one side per message**
  (`buyDepth` = bids, `sellDepth` = asks), 37–50 levels, full replacement of that side.
- Levels are objects `{"price": num, "quantity": num, "sum": num}` — **JSON numbers, not
  strings** (BigDecimal must be parsed from the decimal literal, never via double).
- **No sequence field of any kind** — job 2 has nothing to validate here except arrival order.

Verbatim record (offset 343417, 2026-07-13T08:25:39.637Z):

```json
[
	"TRXUSDT@buyDepth",
	[
		{ "price": 0.32875, "quantity": 1690.4, "sum": 555.719 },
		{ "price": 0.3287, "quantity": 59.4, "sum": 19.52478 },
		{ "price": 0.32818, "quantity": 1497.3, "sum": 491.383914 },
		{ "price": 0.327, "quantity": 4.9, "sum": 1.6023 },
		{ "price": 0.32694, "quantity": 1653.3, "sum": 540.529902 },
		{ "price": 0.3257, "quantity": 1825.6, "sum": 594.59792 },
		{ "price": 0.32542, "quantity": 820.3, "sum": 266.942026 },
		{ "price": 0.32521, "quantity": 1618, "sum": 526.18978 },
		{ "price": 0.325, "quantity": 41.5, "sum": 13.4875 },
		{ "price": 0.32446, "quantity": 2015.8, "sum": 654.046468 },
		{ "price": 0.3244, "quantity": 104.1, "sum": 33.77004 },
		{ "price": 0.32322, "quantity": 2225.9, "sum": 719.455398 },
		{ "price": 0.322, "quantity": 12.5, "sum": 4.025 },
		{ "price": 0.315, "quantity": 33.5, "sum": 10.5525 },
		{ "price": 0.31, "quantity": 8, "sum": 2.48 },
		{ "price": 0.305, "quantity": 10, "sum": 3.05 },
		{ "price": 0.3, "quantity": 51.2, "sum": 15.36 },
		{ "price": 0.2999, "quantity": 30, "sum": 8.997 },
		{ "price": 0.2758, "quantity": 1229.5, "sum": 339.0961 },
		{ "price": 0.273, "quantity": 87.2, "sum": 23.8056 },
		{ "price": 0.26605, "quantity": 1104.2, "sum": 293.77241 },
		{ "price": 0.25777, "quantity": 7, "sum": 1.80439 },
		{ "price": 0.25, "quantity": 307, "sum": 76.75 },
		{ "price": 0.23777, "quantity": 7, "sum": 1.66439 },
		{ "price": 0.22585, "quantity": 2257.4, "sum": 509.83379 },
		{ "price": 0.22, "quantity": 96.3, "sum": 21.186 },
		{ "price": 0.21777, "quantity": 7, "sum": 1.52439 },
		{ "price": 0.2119, "quantity": 126.2, "sum": 26.74178 },
		{ "price": 0.2, "quantity": 225, "sum": 45 },
		{ "price": 0.1866, "quantity": 57.8, "sum": 10.78548 },
		{ "price": 0.17536, "quantity": 50, "sum": 8.768 },
		{ "price": 0.17526, "quantity": 2841.4, "sum": 497.983764 },
		{ "price": 0.14444, "quantity": 700, "sum": 101.108 },
		{ "price": 0.09, "quantity": 1111.1, "sum": 99.999 },
		{ "price": 0.03, "quantity": 3210, "sum": 96.3 },
		{ "price": 0.0001, "quantity": 40000, "sum": 4 },
		{ "price": 0.00001, "quantity": 100000, "sum": 1 }
	]
]
```

Verbatim record (offset 343241, 2026-07-13T08:25:38.711Z):

```json
[
	"TRXTMN@sellDepth",
	[
		{ "price": 59808, "quantity": 92, "sum": 5502336 },
		{ "price": 59809, "quantity": 69.9, "sum": 4180649.1 },
		{ "price": 59810, "quantity": 2937.3, "sum": 175679913 },
		{ "price": 59835, "quantity": 34, "sum": 2034390 },
		{ "price": 60138, "quantity": 2214.4, "sum": 133169587.2 },
		{ "price": 60139, "quantity": 10, "sum": 601390 },
		{ "price": 60140, "quantity": 500, "sum": 30070000 },
		{ "price": 60141, "quantity": 1, "sum": 60141 },
		{ "price": 60142, "quantity": 87.8, "sum": 5280467.6 },
		{ "price": 60143, "quantity": 10, "sum": 601430 },
		{ "price": 60144, "quantity": 10, "sum": 601440 },
		{ "price": 60145, "quantity": 10, "sum": 601450 },
		{ "price": 60146, "quantity": 344, "sum": 20690224 },
		{ "price": 60147, "quantity": 6.6, "sum": 396970.2 },
		{ "price": 60148, "quantity": 779.5, "sum": 46885366 },
		{ "price": 60150, "quantity": 229, "sum": 13774350 },
		{ "price": 60198, "quantity": 187.4, "sum": 11281105.2 },
		{ "price": 60200, "quantity": 420, "sum": 25284000 },
		{ "price": 60295, "quantity": 1, "sum": 60295 },
		{ "price": 60300, "quantity": 391.9, "sum": 23631570 },
		{ "price": 60340, "quantity": 1, "sum": 60340 },
		{ "price": 60350, "quantity": 1, "sum": 60350 },
		{ "price": 60400, "quantity": 1, "sum": 60400 },
		{ "price": 60430, "quantity": 1, "sum": 60430 },
		{ "price": 60450, "quantity": 1, "sum": 60450 },
		{ "price": 60500, "quantity": 43.4, "sum": 2625700 },
		{ "price": 60530, "quantity": 2.5, "sum": 151325 },
		{ "price": 60550, "quantity": 1, "sum": 60550 },
		{ "price": 60600, "quantity": 18.8, "sum": 1139280 },
		{ "price": 60650, "quantity": 1, "sum": 60650 },
		{ "price": 60700, "quantity": 1, "sum": 60700 },
		{ "price": 60743, "quantity": 1, "sum": 60743 },
		{ "price": 60748, "quantity": 184, "sum": 11177632 },
		{ "price": 60749, "quantity": 667.3, "sum": 40537807.7 },
		{ "price": 60750, "quantity": 1, "sum": 60750 },
		{ "price": 60850, "quantity": 1, "sum": 60850 },
		{ "price": 60900, "quantity": 13.7, "sum": 834330 },
		{ "price": 60950, "quantity": 1, "sum": 60950 },
		{ "price": 60975, "quantity": 1, "sum": 60975 },
		{ "price": 60986, "quantity": 161, "sum": 9818746 },
		{ "price": 60988, "quantity": 14.2, "sum": 866029.6 },
		{ "price": 61000, "quantity": 4332, "sum": 264252000 },
		{ "price": 61030, "quantity": 1, "sum": 61030 },
		{ "price": 61035, "quantity": 1, "sum": 61035 },
		{ "price": 61050, "quantity": 1, "sum": 61050 },
		{ "price": 61080, "quantity": 1, "sum": 61080 },
		{ "price": 61100, "quantity": 1, "sum": 61100 },
		{ "price": 61125, "quantity": 1, "sum": 61125 },
		{ "price": 61150, "quantity": 1, "sum": 61150 },
		{ "price": 61170, "quantity": 1, "sum": 61170 }
	]
]
```

---

## ex4-raw — ramzinex

- Channel `orderbook:{MARKET}` where `MARKET` is ramzinex's numeric pair id (`exchange_markets.market`
  stores these digits, e.g. `2`, `13`, `218`).
- `data`: `buys`/`sells` = arrays of 7-element rows, all **JSON numbers**:
  `[price, quantity, sum, false, null, N, timestamp_ms]` — elements 4–6 unverified (likely
  is-own-order flag, ?, order-count); 50 levels per side (occasionally fewer). Full snapshot every message.

Verbatim record (offset 65970, 2026-07-13T08:25:32.05Z):

```json
{
	"push": {
		"channel": "orderbook:27",
		"pub": {
			"data": {
				"buys": [
					[
						0.32851,
						942.717181,
						309.69202113031,
						false,
						null,
						48,
						1783931131960
					],
					[
						0.32564,
						584.91366,
						190.4712842424,
						false,
						null,
						41,
						1783930941273
					],
					[
						0.32377,
						3.8994,
						1.262508738,
						false,
						null,
						7,
						1783930941273
					],
					[
						0.32239,
						496.09747,
						159.9368633533,
						false,
						null,
						38,
						1783930941273
					],
					[0.31347, 1000, 313.47, false, null, 48, 1783930941273],
					[
						0.31262,
						566.80256,
						177.1938163072,
						false,
						null,
						40,
						1783930941273
					],
					[0.30041, 1000, 300.41, false, null, 48, 1783930941273],
					[
						0.29959,
						468.29757,
						140.2972689963,
						false,
						null,
						36,
						1783930941273
					],
					[0.2266, 12, 2.7192, false, null, 9, 1783930941273],
					[0.2265, 100, 22.65, false, null, 19, 1783930941273],
					[0.22, 20, 4.4, false, null, 11, 1783930941273],
					[0.18, 10, 1.8, false, null, 8, 1783930941273],
					[0.1, 332.4, 33.24, false, null, 22, 1783930941273],
					[
						0.07309,
						22.4463,
						1.640600067,
						false,
						null,
						8,
						1783930941273
					],
					[
						0.01519,
						87.7419,
						1.332799461,
						false,
						null,
						7,
						1783930941273
					],
					[
						0.01517,
						236.1239,
						3.581999563,
						false,
						null,
						10,
						1783930941273
					],
					[
						0.01516,
						95.6303,
						1.4497553479999998,
						false,
						null,
						7,
						1783930941273
					],
					[
						0.01515,
						95.6303,
						1.4487990450000001,
						false,
						null,
						7,
						1783930941273
					],
					[
						0.00047,
						12310.6382,
						5.785999954,
						false,
						null,
						12,
						1783930941273
					],
					[
						0.00044,
						43795.4545,
						19.269999979999998,
						false,
						null,
						18,
						1783930941273
					],
					[
						0.00042,
						33952.380899,
						14.25999997758,
						false,
						null,
						16,
						1783930941273
					],
					[
						0.00041,
						88122.8571,
						36.130371411,
						false,
						null,
						23,
						1783930941273
					]
				],
				"sells": [
					[70000, 1.0139, 70973, false, null, 323, 1783930941273],
					[44800, 0.005, 224, false, null, 43, 1783930941273],
					[42450, 0.0113, 479.685, false, null, 56, 1783930941273],
					[34656, 0.2801, 9707.1456, false, null, 161, 1783930941273],
					[11000, 3.9708, 43678.8, false, null, 272, 1783930941273],
					[270, 2.284, 616.68, false, null, 61, 1783930941273],
					[28, 7.3792, 206.6176, false, null, 42, 1783930941273],
					[15, 1.4574, 21.861, false, null, 19, 1783930941273],
					[
						14.99982,
						28.2469,
						423.69841555799997,
						false,
						null,
						54,
						1783930941273
					],
					[12, 12, 144, false, null, 37, 1783930941273],
					[5, 692.8425, 3464.2125, false, null, 112, 1783930941273],
					[4, 11.45, 45.8, false, null, 25, 1783930941273],
					[3, 8.2487, 24.7461, false, null, 20, 1783930941273],
					[2, 2.5776, 5.1552, false, null, 11, 1783930941273],
					[1.9, 1002.424, 1904.6056, false, null, 91, 1783930941273],
					[
						1.04,
						3.7421,
						3.8917840000000004,
						false,
						null,
						10,
						1783930941273
					],
					[1, 52.5563, 52.5563, false, null, 26, 1783930941273],
					[0.9, 100.7472, 90.67248, false, null, 31, 1783930941273],
					[0.75, 17.6295, 13.222125, false, null, 16, 1783930941273],
					[
						0.64123,
						16.3775,
						10.501744325,
						false,
						null,
						15,
						1783930941273
					],
					[0.55, 15, 8.25, false, null, 14, 1783930941273],
					[
						0.525,
						24.1677,
						12.6880425,
						false,
						null,
						16,
						1783930941273
					],
					[
						0.52068,
						13.1815,
						6.86334342,
						false,
						null,
						13,
						1783930941273
					],
					[
						0.50777,
						3.7725,
						1.9155623250000002,
						false,
						null,
						8,
						1783930941273
					],
					[
						0.50126,
						26.4442,
						13.255419691999998,
						false,
						null,
						16,
						1783930941273
					],
					[0.5, 24.1676, 12.0838, false, null, 15, 1783930941273],
					[
						0.49999,
						88.8341,
						44.416161659000004,
						false,
						null,
						24,
						1783930941273
					],
					[0.44, 19.9367, 8.772148, false, null, 14, 1783930941273],
					[0.43, 895.553, 385.08779, false, null, 52, 1783930941273],
					[0.42, 228.358, 95.91036, false, null, 32, 1783930941273],
					[
						0.413,
						1693.3795,
						699.3657334999999,
						false,
						null,
						64,
						1783930941273
					],
					[0.38, 12.0349, 4.573262, false, null, 11, 1783930941273],
					[
						0.3712,
						92485.200383,
						34330.5063821696,
						false,
						null,
						250,
						1783930941273
					],
					[
						0.36435,
						98107.199672,
						35745.3582004932,
						false,
						null,
						254,
						1783930941273
					],
					[
						0.364,
						72155.353536,
						26264.548687104,
						false,
						null,
						228,
						1783930941273
					],
					[
						0.36133,
						303.89103,
						109.80494586990001,
						false,
						null,
						33,
						1783930941273
					],
					[
						0.361,
						24881.689751,
						8982.290000111001,
						false,
						null,
						156,
						1783930941273
					],
					[
						0.36,
						516.8892,
						186.08011199999999,
						false,
						null,
						40,
						1783930941273
					],
					[
						0.356,
						28.2217,
						10.0469252,
						false,
						null,
						15,
						1783930941273
					],
					[
						0.3501,
						28325.700087,
						9916.827600458699,
						false,
						null,
						162,
						1783930941273
					],
					[0.35, 6.9825, 2.443875, false, null, 9, 1783930941273],
					[
						0.3499,
						11375.139724,
						3980.1613894275997,
						false,
						null,
						118,
						1783930941273
					],
					[
						0.34794,
						440.24558,
						153.1790471052,
						false,
						null,
						38,
						1783930941273
					],
					[
						0.338,
						9915.651184,
						3351.490100192,
						false,
						null,
						111,
						1783930941273
					],
					[0.33799, 545, 184.20455, false, null, 40, 1783930941273],
					[
						0.33791,
						369.15494,
						124.74114577540001,
						false,
						null,
						35,
						1783930941273
					],
					[
						0.33537,
						51.004,
						17.10521148,
						false,
						null,
						17,
						1783930941273
					],
					[
						0.33456,
						584.91366,
						195.68871408959998,
						false,
						null,
						41,
						1783930941273
					],
					[
						0.334,
						60.0609,
						20.0603406,
						false,
						null,
						18,
						1783930941273
					],
					[
						0.33399,
						316.5587,
						105.727440213,
						false,
						null,
						33,
						1783930941273
					]
				]
			},
			"offset": 7794547
		}
	}
}
```

---

## ex5-raw — bitget

- Native bitget v2 websocket frame: `action`, `arg{instType,channel,instId}`, `data[0]` with
  `asks`/`bids` (string pairs, 50×50), `ts` (string, ms), `seq`, `pseq`.
- `channel` = `books50`. **All 200 sampled messages are `action:"snapshot"` with `pseq:0`** —
  either NiFi requests snapshots only, or `update` frames exist outside this window. The bitget
  protocol also defines `action:"update"` (with `pseq` chaining to previous `seq`); **not observed**.
- ⚠ Anomaly: per `instId`, `seq` is **not monotonic in topic order** (e.g. ETHUSDT
  `…855106 → …856449 → …850557`). Suggests out-of-order production (multiple producer
  threads/connections?). Must be understood before job 2's sequence rules are applied here.

Verbatim record (offset 183311, 2026-07-13T08:25:45.613Z):

```json
{
	"action": "snapshot",
	"arg": { "instType": "SPOT", "channel": "books50", "instId": "SOLUSDT" },
	"data": [
		{
			"asks": [
				["76.46", "170.4979"],
				["76.47", "157.7384"],
				["76.48", "615.7737"],
				["76.49", "1140.6579"],
				["76.5", "182.099"],
				["76.51", "627.9033"],
				["76.52", "624.5865"],
				["76.53", "55.1059"],
				["76.54", "115.2946"],
				["76.55", "324.2526"],
				["76.56", "444.9932"],
				["76.57", "109.3573"],
				["76.58", "70.1361"],
				["76.59", "123.3304"],
				["76.6", "484.5297"],
				["76.61", "1538.7692"],
				["76.62", "105.8116"],
				["76.63", "53.889"],
				["76.64", "42.9299"],
				["76.65", "1455.9054"],
				["76.66", "41.875"],
				["76.67", "102.3301"],
				["76.68", "19.3473"],
				["76.69", "1245.5286"],
				["76.7", "44.8301"],
				["76.71", "1752.0342"],
				["76.72", "1507.3683"],
				["76.73", "1562.6898"],
				["76.74", "42.3818"],
				["76.75", "46.3257"],
				["76.76", "42.5273"],
				["76.77", "1387.9849"],
				["76.78", "89.5718"],
				["76.79", "1379.2252"],
				["76.8", "49.0401"],
				["76.81", "44.5152"],
				["76.82", "44.001"],
				["76.83", "42.2854"],
				["76.84", "42.8768"],
				["76.85", "45.5254"],
				["76.86", "44.4216"],
				["76.87", "42.2063"],
				["76.88", "26.7454"],
				["76.89", "25.4036"],
				["76.9", "28.0739"],
				["76.91", "25.8868"],
				["76.92", "26.5738"],
				["76.93", "26.3946"],
				["76.94", "26.9588"],
				["76.95", "26.4224"]
			],
			"bids": [
				["76.45", "51.5912"],
				["76.44", "479.8938"],
				["76.43", "915.0479"],
				["76.42", "289.525"],
				["76.41", "825.7275"],
				["76.4", "844.8768"],
				["76.39", "504.3807"],
				["76.38", "410.2058"],
				["76.37", "774.379"],
				["76.36", "453.7381"],
				["76.35", "183.4227"],
				["76.34", "633.5887"],
				["76.33", "5348.0441"],
				["76.32", "162.9463"],
				["76.31", "1775.7804"],
				["76.3", "42.0497"],
				["76.29", "79.9366"],
				["76.28", "43.1005"],
				["76.27", "103.9208"],
				["76.26", "1843.9421"],
				["76.25", "43.9818"],
				["76.24", "42.9056"],
				["76.23", "5031.9493"],
				["76.22", "2204.0303"],
				["76.21", "2078.9875"],
				["76.2", "83.1337"],
				["76.19", "82.2922"],
				["76.18", "44.6093"],
				["76.17", "42.4303"],
				["76.16", "42.17"],
				["76.15", "45.3369"],
				["76.14", "44.059"],
				["76.13", "245.6265"],
				["76.12", "44.1419"],
				["76.11", "46.2055"],
				["76.1", "49.0074"],
				["76.09", "42.8702"],
				["76.08", "25.2441"],
				["76.07", "25.3952"],
				["76.06", "25.3929"],
				["76.05", "26.5849"],
				["76.04", "25.4138"],
				["76.03", "25.5596"],
				["76.02", "25.3974"],
				["76.01", "26.1239"],
				["76", "39.3047"],
				["75.99", "25.216"],
				["75.98", "459.1078"],
				["75.97", "26.6711"],
				["75.96", "1094.7464"]
			],
			"ts": "1783931143931",
			"seq": 748071887651,
			"pseq": 0
		}
	],
	"ts": 1783931143933
}
```

---

## ex6-raw — bybit

- Native bybit v5 frame: `topic: "orderbook.50.{MARKET}"`, `type: "delta"`, `data{s, b, a, u, seq}`,
  `ts`/`cts` (ms). `b`/`a` = changed levels only (string pairs, 0–70 per side, median 2);
  **`quantity:"0"` means delete the level**.
- Protocol also defines `type:"snapshot"` (sent on subscribe/reconnect); **not observed in the
  latest 200** — a snapshot capture is still needed for fixtures.
- ⚠ Anomaly: per symbol, `u` (update id) shows frequent gaps in topic order (e.g. 17 gaps across
  53 SOLUSDT messages). If bybit guarantees contiguous `u` per symbol on one connection, messages
  are being lost before/at NiFi. Directly affects job 2's gap rules — verify.

Verbatim record (offset 313000, 2026-07-13T08:25:48.594Z):

```json
{
	"topic": "orderbook.50.SOLUSDT",
	"ts": 1783931148445,
	"type": "delta",
	"data": {
		"s": "SOLUSDT",
		"b": [["76.41", "173.8305"]],
		"a": [["76.5", "764.1611"]],
		"u": 76414790,
		"seq": 155262345824
	},
	"cts": 1783931148441
}
```

Delta containing `"0"` quantities (level deletions):

Verbatim record (offset 313020, 2026-07-13T08:25:48.911Z):

```json
{
	"topic": "orderbook.50.BTCUSDT",
	"ts": 1783931148736,
	"type": "delta",
	"data": {
		"s": "BTCUSDT",
		"b": [
			["63015.8", "0.0005"],
			["63012.1", "0"]
		],
		"a": [],
		"u": 124414028,
		"seq": 111360615093
	},
	"cts": 1783931148729
}
```

---

## ex7-raw — ompfinex

- Centrifugo channel `public-market:r-depth-{MARKET}`, numeric market id (`exchange_markets.market`).
- `data`: Binance-style diff-depth: `U` (first update id), `u` (final update id), `a`/`b` =
  changed levels as `["price","quantity"]` strings (0–9 per side); **`quantity:"0"` = delete**.
- Delta-only in the sample; no snapshot variant observed — how the initial book is obtained is
  an open question for job 5's cold start.
- ⚠ Anomaly: on some channels consecutive messages have `U > prev_u + 1` (missing ranges), e.g.
  `r-depth-27`: `(…433,…436) → next U …438`. Same verification need as ex6.

Verbatim record (offset 26608, 2026-07-13T08:25:24.453Z):

```json
{
	"push": {
		"channel": "public-market:r-depth-21",
		"pub": {
			"data": {
				"u": 2411192,
				"U": 2411191,
				"a": [["0.3302", "0"]],
				"b": [["0.3302", "2.13"]]
			},
			"offset": 94120
		}
	}
}
```

---

## Consequences for the pipeline (feed into todo.md M0)

1. **Job 1 must split multi-document records** (seen on ex1; assume possible anywhere).
2. **Three snapshot/update regimes**, not one:
   full-snapshot-every-message (ex1, ex2, ex4), full-snapshot-per-SIDE (ex3),
   delta-with-sequence (ex5, ex6, ex7 — plus a snapshot bootstrap that was NOT observed for
   ex6/ex7). The shared post-job-1 Avro event and job 2/5 semantics must cover all three.
3. **wallex (ex3) and ramzinex (ex4) put prices/quantities on the wire as JSON numbers** — parse
   into BigDecimal from the literal (Jackson `USE_BIG_DECIMAL_FOR_FLOATS`), never via double.
4. **ex3 has no sequence data at all**; ex1/ex2/ex4 only have the Centrifugo `pub.offset`.
5. **Sequence anomalies to investigate before job 2**: bitget non-monotonic `seq`,
   bybit `u` gaps, ompfinex `U`/`u` range gaps.
6. No `ex8-raw` (okx) yet, but the DB already defines the exchange — pipeline should tolerate a
   new exchange appearing.
