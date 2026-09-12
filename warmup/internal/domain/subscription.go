// Package domain holds the few types the warmup steps pass between each other.
package domain

// Subscription is one row of exchange_markets: a pair traded on an exchange.
// The two ids are what every topic name is built from.
//
// ⚠ exchange_markets is UNIQUE on (exchange_id, market) — the exchange's own
// symbol string — NOT on (exchange_id, market_id), so two rows can carry the
// same pair of ids.
type Subscription struct {
	PairID     int64
	ExchangeID int64
}
