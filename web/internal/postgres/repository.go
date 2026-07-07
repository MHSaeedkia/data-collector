// Package postgres implements ports.MarketRepository against a real
// postgres connection pool. It is a thin adapter with no branching logic
// of its own, so it is not unit-tested; the merge/fallback logic it feeds
// lives in internal/registry and is tested there against a fake.
package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"orderbook-web/internal/domain"
)

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) LoadMarkets(ctx context.Context) (map[int]domain.Market, error) {
	rows, err := r.pool.Query(ctx, "SELECT m.id, b.name, q.name FROM markets m JOIN currencies b ON m.base_id = b.id JOIN currencies q ON m.quote_id = q.id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	markets := map[int]domain.Market{}
	for rows.Next() {
		var m domain.Market
		if err := rows.Scan(&m.ID, &m.Base, &m.Quote); err == nil {
			markets[m.ID] = m
		}
	}
	return markets, rows.Err()
}

func (r *Repository) LoadExchanges(ctx context.Context) (map[int]domain.Exchange, error) {
	rows, err := r.pool.Query(ctx, "SELECT id, name, label FROM exchanges")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	exchanges := map[int]domain.Exchange{}
	for rows.Next() {
		var e domain.Exchange
		if err := rows.Scan(&e.ID, &e.Name, &e.Label); err == nil {
			exchanges[e.ID] = e
		}
	}
	return exchanges, rows.Err()
}
