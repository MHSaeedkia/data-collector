package registry

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"orderbook-web/internal/domain"
)

// fakeRepo lets tests control exactly what each load call returns,
// including simulating a partial failure.
type fakeRepo struct {
	markets     map[int]domain.Market
	marketsErr  error
	exchanges   map[int]domain.Exchange
	exchangeErr error
}

func (f *fakeRepo) LoadMarkets(ctx context.Context) (map[int]domain.Market, error) {
	return f.markets, f.marketsErr
}

func (f *fakeRepo) LoadExchanges(ctx context.Context) (map[int]domain.Exchange, error) {
	return f.exchanges, f.exchangeErr
}

func TestRefresh_PopulatesBothMaps(t *testing.T) {
	repo := &fakeRepo{
		markets:   map[int]domain.Market{1: {ID: 1, Base: "BTC", Quote: "USDT"}},
		exchanges: map[int]domain.Exchange{2: {ID: 2, Name: "nobitex", Label: "نوبیتکس"}},
	}
	r := New(repo)

	r.Refresh(context.Background())

	got := r.Enrich(domain.RawBook{PairID: 1, Side: "asks", Levels: []domain.RawLevel{{ExchangeID: 2, Price: "1", Quantity: "2"}}})
	assert.Equal(t, "BTC", got.Base)
	assert.Equal(t, "USDT", got.Quote)
	assert.Equal(t, "nobitex", got.Levels[0].Exchange.Name)
}

func TestRefresh_TransientErrorDoesNotBlankExistingData(t *testing.T) {
	repo := &fakeRepo{
		markets:   map[int]domain.Market{1: {ID: 1, Base: "BTC", Quote: "USDT"}},
		exchanges: map[int]domain.Exchange{2: {ID: 2, Name: "nobitex", Label: "نوبیتکس"}},
	}
	r := New(repo)
	r.Refresh(context.Background())

	// Second refresh fails on both queries; a real error case (empty map + error).
	repo.markets = nil
	repo.marketsErr = errors.New("connection reset")
	repo.exchanges = nil
	repo.exchangeErr = errors.New("connection reset")
	r.Refresh(context.Background())

	got := r.Enrich(domain.RawBook{PairID: 1, Side: "asks", Levels: []domain.RawLevel{{ExchangeID: 2, Price: "1", Quantity: "2"}}})
	require.Equal(t, "BTC", got.Base, "previously loaded market data must survive a failed refresh")
	assert.Equal(t, "nobitex", got.Levels[0].Exchange.Name, "previously loaded exchange data must survive a failed refresh")
}

func TestEnrich_UnknownIdsFallBackToPlaceholders(t *testing.T) {
	r := New(&fakeRepo{})

	got := r.Enrich(domain.RawBook{
		PairID: 42,
		Side:   "bids",
		Levels: []domain.RawLevel{{ExchangeID: 7, Price: "1.5", Quantity: "3"}},
	})

	assert.Equal(t, "p42", got.Base)
	assert.Equal(t, "?", got.Quote)
	require.Len(t, got.Levels, 1)
	assert.Equal(t, "unknown", got.Levels[0].Exchange.Name)
	assert.Equal(t, "نامشخص", got.Levels[0].Exchange.Label)
}

func TestEnrich_PreservesLevelOrderAndFields(t *testing.T) {
	repo := &fakeRepo{
		exchanges: map[int]domain.Exchange{
			1: {ID: 1, Name: "ex1", Label: "Exchange One"},
			2: {ID: 2, Name: "ex2", Label: "Exchange Two"},
		},
	}
	r := New(repo)
	r.Refresh(context.Background())

	got := r.Enrich(domain.RawBook{
		PairID: 1,
		Side:   "asks",
		Levels: []domain.RawLevel{
			{ExchangeID: 1, Price: "100", Quantity: "1"},
			{ExchangeID: 2, Price: "101", Quantity: "2"},
		},
		EventTime: 123,
	})

	require.Len(t, got.Levels, 2)
	assert.Equal(t, "ex1", got.Levels[0].Exchange.Name)
	assert.Equal(t, "ex2", got.Levels[1].Exchange.Name)
	assert.Equal(t, int64(123), got.EventTime)
}
