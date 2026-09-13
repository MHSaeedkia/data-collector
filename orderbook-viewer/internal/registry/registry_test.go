package registry

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"orderbook-viewer/internal/domain"
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
	r := New(repo, domain.Defaults{})

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
	r := New(repo, domain.Defaults{})
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
	r := New(&fakeRepo{}, domain.Defaults{})

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

// A per-exchange book names its source exchange at the book level — that
// is what the browser routes on — while the per-level exchange stays
// filled so the table renders the same either way.
func TestEnrich_PerExchangeBookResolvesTheBookLevelExchange(t *testing.T) {
	repo := &fakeRepo{exchanges: map[int]domain.Exchange{8: {ID: 8, Name: "okx", Label: "OKX"}}}
	r := New(repo, domain.Defaults{})
	r.Refresh(context.Background())

	got := r.Enrich(domain.RawBook{
		PairID:     1,
		ExchangeID: 8,
		Side:       "asks",
		Levels:     []domain.RawLevel{{ExchangeID: 8, Price: "100", Quantity: "1"}},
	})

	require.NotNil(t, got.Exchange)
	assert.Equal(t, "okx", got.Exchange.Name)
	assert.Equal(t, "okx", got.Levels[0].Exchange.Name)
}

func TestEnrich_AggregatedBookHasNoBookLevelExchange(t *testing.T) {
	r := New(&fakeRepo{}, domain.Defaults{})

	got := r.Enrich(domain.RawBook{PairID: 1, Side: "asks"})

	assert.Nil(t, got.Exchange, "the aggregated book belongs to no single exchange")
	assert.Equal(t, domain.AggregatedExchangeID, got.ExchangeID())
}

func TestEnrich_MergedBookResolvesEveryContributingExchange(t *testing.T) {
	repo := &fakeRepo{exchanges: map[int]domain.Exchange{
		1: {ID: 1, Name: "nobitex", Label: "نوبیتکس"},
		3: {ID: 3, Name: "wallex", Label: "والکس"},
	}}
	r := New(repo, domain.Defaults{})
	r.Refresh(context.Background())

	got := r.Enrich(domain.RawBook{
		PairID: 1,
		Merged: true,
		Side:   "asks",
		Levels: []domain.RawLevel{{ExchangeIDs: []int{1, 3}, Price: "100", Quantity: "14"}},
	})

	assert.True(t, got.Merged)
	assert.Nil(t, got.Exchange, "merged is a view across exchanges, not an exchange")
	assert.Equal(t, domain.MergedExchangeID, got.ExchangeID())
	require.Len(t, got.Levels[0].Exchanges, 2)
	assert.Equal(t, "nobitex", got.Levels[0].Exchanges[0].Name)
	assert.Equal(t, "wallex", got.Levels[0].Exchanges[1].Name)
	// Resolving the absent scalar id would stamp every merged row with the
	// "unknown" placeholder, which is worse than showing nothing there.
	assert.Empty(t, got.Levels[0].Exchange.Name)
}

func TestEnrich_MergedUnknownExchangeStillFallsBackToThePlaceholder(t *testing.T) {
	r := New(&fakeRepo{exchanges: map[int]domain.Exchange{1: {ID: 1, Name: "nobitex"}}}, domain.Defaults{})
	r.Refresh(context.Background())

	got := r.Enrich(domain.RawBook{
		PairID: 1,
		Merged: true,
		Side:   "asks",
		Levels: []domain.RawLevel{{ExchangeIDs: []int{1, 99}, Price: "100", Quantity: "14"}},
	})

	require.Len(t, got.Levels[0].Exchanges, 2, "an id postgres doesn't know must not drop a contributor")
	assert.Equal(t, "unknown", got.Levels[0].Exchanges[1].Name)
}

func TestCatalog_ListsEverythingSortedByID(t *testing.T) {
	repo := &fakeRepo{
		markets: map[int]domain.Market{
			2: {ID: 2, Base: "ETH", Quote: "USDT"},
			1: {ID: 1, Base: "BTC", Quote: "USDT"},
		},
		exchanges: map[int]domain.Exchange{
			8: {ID: 8, Name: "okx"},
			1: {ID: 1, Name: "nobitex"},
		},
	}
	r := New(repo, domain.Defaults{})
	r.Refresh(context.Background())

	got := r.Catalog()

	require.Len(t, got.Markets, 2)
	assert.Equal(t, []int{1, 2}, []int{got.Markets[0].ID, got.Markets[1].ID})
	require.Len(t, got.Exchanges, 2)
	assert.Equal(t, []int{1, 8}, []int{got.Exchanges[0].ID, got.Exchanges[1].ID})
}

func TestEnrich_PreservesLevelOrderAndFields(t *testing.T) {
	repo := &fakeRepo{
		exchanges: map[int]domain.Exchange{
			1: {ID: 1, Name: "ex1", Label: "Exchange One"},
			2: {ID: 2, Name: "ex2", Label: "Exchange Two"},
		},
	}
	r := New(repo, domain.Defaults{})
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

// The defaults are database ids — the vocabulary the whole platform
// speaks — so the registry's job is to check that postgres actually has
// them, not to translate anything.
func TestCatalog_KeepsAConfiguredPairIDThatExists(t *testing.T) {
	repo := &fakeRepo{markets: map[int]domain.Market{
		1: {ID: 1, Base: "BTC", Quote: "USDT"},
		2: {ID: 2, Base: "ETH", Quote: "USDT"},
	}}
	r := New(repo, domain.Defaults{PairID: 2})
	r.Refresh(context.Background())

	assert.Equal(t, 2, r.Catalog().DefaultPairID)
}

// 0 is not a market id, so it is the page's cue to fall back to the first
// market — which is what it did before any of this was configurable.
func TestCatalog_UnknownOrUnsetDefaultPairIsZero(t *testing.T) {
	repo := &fakeRepo{markets: map[int]domain.Market{1: {ID: 1, Base: "BTC", Quote: "USDT"}}}

	for name, defaults := range map[string]domain.Defaults{
		"unset":    {},
		"unknown":  {PairID: 99},
		"negative": {PairID: -3},
	} {
		t.Run(name, func(t *testing.T) {
			r := New(repo, defaults)
			r.Refresh(context.Background())

			assert.Zero(t, r.Catalog().DefaultPairID)
		})
	}
}

// The exchange dropdown offers two cross-exchange views alongside the
// real exchanges, and the exchange_id vocabulary already names them, so
// DEFAULT_EXCHANGE_ID reaches them with no special spelling.
func TestCatalog_ChecksTheConfiguredDefaultExchangeID(t *testing.T) {
	repo := &fakeRepo{exchanges: map[int]domain.Exchange{8: {ID: 8, Name: "OKX"}}}

	for name, tc := range map[string]struct {
		configured int
		want       int
	}{
		"unset":               {0, domain.AggregatedExchangeID},
		"separated":           {domain.AggregatedExchangeID, domain.AggregatedExchangeID},
		"merged":              {domain.MergedExchangeID, domain.MergedExchangeID},
		"a real exchange":     {8, 8},
		"an exchange we lack": {99, domain.AggregatedExchangeID},
		"nonsense":            {-7, domain.AggregatedExchangeID},
	} {
		t.Run(name, func(t *testing.T) {
			r := New(repo, domain.Defaults{ExchangeID: tc.configured})
			r.Refresh(context.Background())

			assert.Equal(t, tc.want, r.Catalog().DefaultExchangeID)
		})
	}
}

// A cold start with postgres down cannot check an id, and must not pick
// something arbitrary instead. Once the maps arrive the default starts
// working, and the catalog changing is what re-broadcasts it.
func TestCatalog_ChecksTheDefaultsOncePostgresAnswers(t *testing.T) {
	repo := &fakeRepo{marketsErr: errors.New("connection refused")}
	r := New(repo, domain.Defaults{PairID: 7})
	r.Refresh(context.Background())
	require.Zero(t, r.Catalog().DefaultPairID)

	repo.marketsErr = nil
	repo.markets = map[int]domain.Market{7: {ID: 7, Base: "BTC", Quote: "USDT"}}
	r.Refresh(context.Background())

	assert.Equal(t, 7, r.Catalog().DefaultPairID)
}

// The page reads every dropdown's contents and starting value out of the
// catalog, so all of it has to be in there.
func TestCatalog_CarriesTheDepthChoicesAndTheDefault(t *testing.T) {
	r := New(&fakeRepo{}, domain.Defaults{LevelLimit: 50})

	c := r.Catalog()

	assert.Equal(t, domain.LevelLimits, c.LevelLimits)
	assert.Equal(t, 50, c.DefaultLevelLimit)
}
