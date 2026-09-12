// Package postgres reads the subscription list the topic plan is built from.
// It is a thin adapter with no branching logic of its own, so it is not
// unit-tested; the planning it feeds lives in internal/topics and is tested
// there.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"orderbook-warmup/internal/domain"
)

// subscriptionsQuery lists the pair/exchange combinations, as the shell warmup
// did through `docker exec postgres psql`.
//
// It deliberately does NOT filter on exchange_markets.status: the shell version
// never did either, so topics exist for unsubscribed rows too.
const subscriptionsQuery = `SELECT m.id, em.exchange_id
        FROM exchange_markets em
        JOIN markets m ON em.market_id = m.id
        JOIN exchanges e ON e.id = em.exchange_id`

// LoadSubscriptions connects to postgres, reads every exchange_markets row and
// disconnects. One query, one connection — a pool would buy nothing in a
// one-shot command.
func LoadSubscriptions(ctx context.Context, dsn string) ([]domain.Subscription, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, subscriptionsQuery)
	if err != nil {
		return nil, fmt.Errorf("query exchange_markets: %w", err)
	}
	defer rows.Close()

	var subs []domain.Subscription
	for rows.Next() {
		var s domain.Subscription
		if err := rows.Scan(&s.PairID, &s.ExchangeID); err != nil {
			return nil, err
		}
		subs = append(subs, s)
	}
	return subs, rows.Err()
}
