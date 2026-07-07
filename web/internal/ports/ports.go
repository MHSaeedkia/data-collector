// Package ports declares the interfaces that the registry depends on, so
// its refresh/merge logic can be unit-tested against a fake instead of a
// real postgres connection. The concrete implementation lives in the
// sibling postgres/ package.
package ports

import (
	"context"

	"orderbook-web/internal/domain"
)

// MarketRepository loads display metadata from the store. Markets and
// exchanges are two separate calls (rather than one combined query) so a
// partial failure (e.g. the exchanges query fails) doesn't discard
// markets that did load.
type MarketRepository interface {
	LoadMarkets(ctx context.Context) (map[int]domain.Market, error)
	LoadExchanges(ctx context.Context) (map[int]domain.Exchange, error)
}
