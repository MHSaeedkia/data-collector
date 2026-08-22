// Package postgres is the only place that knows SQL. It reads exchange_markets joined to
// exchanges, and writes the status column — nothing else in the schema is touched.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"market-subscriptions/internal/domain"
)

// Repository owns the connection pool.
type Repository struct{ pool *pgxpool.Pool }

// New opens a pool and verifies it, so a bad DATABASE_URL fails at startup rather than on
// the first request.
func New(ctx context.Context, databaseURL string) (*Repository, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Repository{pool: pool}, nil
}

func (r *Repository) Close() { r.pool.Close() }

// Ping backs the readiness endpoint.
func (r *Repository) Ping(ctx context.Context) error { return r.pool.Ping(ctx) }

// The one query the list view needs. Ordering is (exchange, market) so "sorted by
// exchange name" — which is how an operator picks a whole exchange to act on — is the
// natural order, not something the UI has to re-sort into.
const listQuery = `
	SELECT em.id, em.exchange_id, e.name, e.label, em.market, em.market_id, em.status
	FROM exchange_markets em
	JOIN exchanges e ON e.id = em.exchange_id
	ORDER BY e.name, em.market`

// List returns every subscription. Filtering is deliberately left to the caller/UI: the
// whole table is a few hundred rows, so paging and server-side filters would be
// complexity with nothing to buy.
func (r *Repository) List(ctx context.Context) ([]domain.Subscription, error) {
	rows, err := r.pool.Query(ctx, listQuery)
	if err != nil {
		return nil, fmt.Errorf("list subscriptions: %w", err)
	}
	defer rows.Close()

	subs := []domain.Subscription{}
	for rows.Next() {
		var s domain.Subscription
		if err := rows.Scan(&s.ID, &s.ExchangeID, &s.ExchangeName, &s.ExchangeLabel,
			&s.Market, &s.MarketID, &s.Status); err != nil {
			return nil, fmt.Errorf("scan subscription: %w", err)
		}
		subs = append(subs, s)
	}
	return subs, rows.Err()
}

// Exchanges backs the UI's exchange filter. Read from the table rather than derived from
// the subscription list so an exchange with no markets still appears.
func (r *Repository) Exchanges(ctx context.Context) ([]domain.Exchange, error) {
	rows, err := r.pool.Query(ctx, `SELECT id, name, label FROM exchanges ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list exchanges: %w", err)
	}
	defer rows.Close()

	out := []domain.Exchange{}
	for rows.Next() {
		var e domain.Exchange
		if err := rows.Scan(&e.ID, &e.Name, &e.Label); err != nil {
			return nil, fmt.Errorf("scan exchange: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Get returns one subscription by id.
func (r *Repository) Get(ctx context.Context, id int64) (domain.Subscription, error) {
	var s domain.Subscription
	err := r.pool.QueryRow(ctx, `
		SELECT em.id, em.exchange_id, e.name, e.label, em.market, em.market_id, em.status
		FROM exchange_markets em
		JOIN exchanges e ON e.id = em.exchange_id
		WHERE em.id = $1`, id).
		Scan(&s.ID, &s.ExchangeID, &s.ExchangeName, &s.ExchangeLabel,
			&s.Market, &s.MarketID, &s.Status)
	if err != nil {
		return domain.Subscription{}, fmt.Errorf("get subscription %d: %w", id, err)
	}
	return s, nil
}

// SetStatus writes the status column. The cast is required: the column is the
// subscription_status ENUM, and pgx sends a Go string as text, which postgres will not
// implicitly coerce.
func (r *Repository) SetStatus(ctx context.Context, id int64, status domain.Status) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE exchange_markets SET status = $1::subscription_status WHERE id = $2`,
		string(status), id)
	if err != nil {
		return fmt.Errorf("set status of %d to %s: %w", id, status, err)
	}
	return nil
}
