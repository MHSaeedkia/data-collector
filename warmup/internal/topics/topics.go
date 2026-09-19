// Package topics turns the subscription list into the set of Kafka topics the
// pipeline needs, and works out what the broker is missing.
package topics

import (
	"fmt"
	"slices"

	"orderbook-warmup/internal/config"
	"orderbook-warmup/internal/domain"
)

// normalizerStages are the raw pipeline's per-(exchange, pair) intermediate stages, one per job
// output. Job 1's output is NOT here: the parser does not know the pair yet, so it writes one
// topic per exchange instead — see the ex%d-parsed-flink block below.
var normalizerStages = []string{
	"raw-flink",                // job 2 pair-extractor out
	"type-validated-raw-flink", // job 3 type-validator out
	"rebased-flink",            // job 4 rebaser        out
	"applied-precision-flink",  // job 5 precision      out
	"orderbook-snapshot-flink", // job 6 book-builder   out
}

// Plan lists every topic the pipeline needs, in creation order.
//
// The normalizer stage topics come first because every normalizer source reads
// from `latest`: a topic that does not exist when its job starts is discovered
// late, and whatever was produced in between is lost.
func Plan(subscriptions []domain.Subscription, r config.Retentions) []domain.Topic {
	// exchange_markets is unique on (exchange_id, market) — the exchange's own
	// symbol string — not on (exchange_id, market_id), so two rows can name the
	// same exchange and pair. Collapsing them here is what keeps one topic out
	// of a create batch twice, which the broker rejects.
	subs := slices.Clone(subscriptions)
	slices.SortFunc(subs, func(a, b domain.Subscription) int {
		if a.ExchangeID != b.ExchangeID {
			return int(a.ExchangeID - b.ExchangeID)
		}
		return int(a.PairID - b.PairID)
	})
	subs = slices.Compact(subs)

	var plan []domain.Topic
	add := func(name, retention string) {
		plan = append(plan, domain.Topic{Name: name, RetentionMS: retention})
	}

	// Control plane — one shared topic, independent of which pairs are
	// subscribed. Created unconditionally since NiFi needs it regardless of how
	// many markets are currently active.
	add("control-plane", r.Control)

	for _, s := range subs {
		prefix := fmt.Sprintf("ex%d-p%d", s.ExchangeID, s.PairID)
		for _, stage := range normalizerStages {
			add(prefix+"-"+stage, r.Input)
		}
		// Shared dead-letter for jobs 3 and 4.
		add(prefix+"-rejected-flink", r.Rejected)
	}

	// Per-exchange topics, both ends of job 1:
	//   ex{id}-raw           NiFi publishes verbatim exchange payloads here
	//   ex{id}-parsed-flink  job 1 parser out — parsed, but the pair is still the exchange's
	//                        own market string, so there is no p{id} segment to put in the name
	// Created alongside the stage topics above and before the outputs, for the same reason: job
	// 2's source reads from `latest`.
	for _, id := range distinct(subs, func(s domain.Subscription) int64 { return s.ExchangeID }) {
		add(fmt.Sprintf("ex%d-raw", id), r.Raw)
		add(fmt.Sprintf("ex%d-parsed-flink", id), r.Input)
	}

	// Output topics — one per pair+side. Three parallel views of the same
	// cross-exchange book:
	//   p{id}-{side}          normalizer job 7 — levels UNIONED, each keeping its own exchange_id
	//   p{id}-{side}-merged   flink/merger     — levels SUMMED, one per price, exchange_ids as a list
	//   p{id}-{side}-adjusted flink/adjustment — job 7's record with commission/profit/slippage applied
	// The -merged and -adjusted families are created here (not in their own
	// projects) for the same reason as every other topic: their sources read
	// from `latest`, so the topic must exist before the job starts.
	for _, id := range distinct(subs, func(s domain.Subscription) int64 { return s.PairID }) {
		for _, side := range []string{"asks", "bids"} {
			add(fmt.Sprintf("p%d-%s", id, side), r.Output)
			add(fmt.Sprintf("p%d-%s-merged", id, side), r.Output)
			add(fmt.Sprintf("p%d-%s-adjusted", id, side), r.Output)
		}
	}

	return plan
}

// Diff splits the plan into topics the broker does not have yet and topics
// whose retention.ms no longer matches the configured one. existing maps the
// name of every topic that is already there to its current retention.ms.
func Diff(desired []domain.Topic, existing map[string]string) (create, alter []domain.Topic) {
	for _, t := range desired {
		current, ok := existing[t.Name]
		switch {
		case !ok:
			create = append(create, t)
		case current == "" || current == t.RetentionMS:
			// Unchanged, or the broker did not report a value — leave it alone
			// rather than alter it on every run.
		default:
			alter = append(alter, t)
		}
	}
	return create, alter
}

func distinct(subs []domain.Subscription, id func(domain.Subscription) int64) []int64 {
	var ids []int64
	for _, s := range subs {
		ids = append(ids, id(s))
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}
