// Package kafka is a thin adapter over franz-go: it owns the client and
// the poll loop, and hands each record's topic/value to a callback. It
// has no branching logic of its own (that lives in internal/ingest,
// tested there against a fake), so it isn't unit-tested here.
package kafka

import (
	"context"
	"errors"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// statsPeriod is how often each consumer reports what it is doing. The
// point of the report is the RATE: a consumer that has stopped consuming
// looks identical to a quiet market in any snapshot of state, and telling
// those two apart by hand is what made the last outage expensive.
const statsPeriod = 30 * time.Second

// purgeCooldown is the minimum gap between two purges of the same topic.
// UNKNOWN_TOPIC_ID repeats on every poll, so without a cooldown one
// recreated topic would fire a purge several times a second.
const purgeCooldown = time.Minute

const (
	// Aggregated output topics: p{pair_id}-{side} (e.g. p2-asks), plus the
	// merger's p{pair_id}-{side}-merged. Per-exchange topics carry a
	// leading ex... so they don't match.
	//
	// The -merged suffix is optional here ON PURPOSE, and this regex is
	// the mirror image of the merger job's own: that one must exclude
	// -merged or it would consume its own output forever, while this one
	// wants both views. Same record rate and same shape of consumer, so
	// they share this client rather than needing a third.
	aggregatedPattern = `^p[0-9]+-(asks|bids)(-merged)?$`
	// Job 5's per-exchange books: one record holds both sides.
	snapshotPattern = `^ex[0-9]+-p[0-9]+-orderbook-snapshot-flink$`
)

type Consumer struct {
	client *kgo.Client
	// name and group are carried only so the log lines say which of the
	// two consumers they came from.
	name  string
	group string

	mu        sync.Mutex
	records   int64
	bytes     int64
	perTopic  map[string]int64
	lastCount int64
	// purged is when each topic was last purged, so a repeating error
	// does not purge on every poll.
	purged map[string]time.Time
}

// NewAggregatedConsumer reads the aggregator's and the merger's output
// from the LATEST offset: live records only, no history.
//
// It used to start at the earliest offset so the book painted on page
// load. That was a dev convenience and it became the app's worst failure
// mode in production: warmup.sh keeps 6 hours on these topics, so every
// restart replayed six hours of full order books at fetch speed and
// pushed each one at the browser, which cannot render that fast. The
// socket backed up and the whole server froze (see internal/hub). The
// trade for reading live only is that a quiet pair shows nothing until
// its next record; the hub's snapshot answer covers everything that has
// arrived since the process started.
func NewAggregatedConsumer(broker string) (*Consumer, error) {
	return newConsumer(broker, "agg", aggregatedPattern, kgo.NewOffset().AtEnd())
}

// NewSnapshotConsumer reads job 5's per-exchange books, also from the
// latest offset. These topics carry a full book on every event, one per
// exchange × pair, so replaying their retention window at startup would
// cost far more than it is worth.
func NewSnapshotConsumer(broker string) (*Consumer, error) {
	return newConsumer(broker, "ex", snapshotPattern, kgo.NewOffset().AtEnd())
}

// newConsumer connects with a fresh consumer group at the given offset.
// Both families now read from the end, so separate clients are no longer
// forced by the client-wide reset offset — they stay apart only because
// they are two independent subscriptions. The group name carries a
// timestamp so every start is a new group: a stable name would resume
// from committed offsets and reintroduce the backlog replay that reading
// from the end exists to avoid.
func newConsumer(broker, group, pattern string, offset kgo.Offset) (*Consumer, error) {
	name := "orderbook-web-" + group + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeRegex(),
		kgo.ConsumeTopics(pattern),
		kgo.ConsumerGroup(name),
		kgo.ConsumeResetOffset(offset),
		// Both patterns this is called with (job 5's snapshots, job 6's aggregated family) are
		// EXACTLY_ONCE/transactional again; franz-go defaults to read_uncommitted, which would let
		// this see records from a transaction that later aborts.
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		return nil, err
	}
	log.Printf("kafka[%s]: consuming %s from the LATEST offset, group %s", group, pattern, name)
	return &Consumer{
		client:   cl,
		name:     group,
		group:    name,
		perTopic: map[string]int64{},
		purged:   map[string]time.Time{},
	}, nil
}

// Run polls until ctx is cancelled, calling onRecord for each fetched
// record. It blocks, so callers run it in a goroutine.
func (c *Consumer) Run(ctx context.Context, onRecord func(topic string, value []byte)) {
	defer c.client.Close()
	go c.logStats(ctx)
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			log.Printf("kafka[%s]: consumer stopped: %v", c.name, ctx.Err())
			return
		}
		fetches.EachError(c.handleFetchErr)
		fetches.EachRecord(func(rec *kgo.Record) {
			c.count(rec)
			onRecord(rec.Topic, rec.Value)
		})
	}
}

// handleFetchErr logs a fetch error and recovers from the one kind that
// never recovers on its own.
//
// franz-go pins a topic's ID when it first creates the cursor and
// deliberately never adopts a new one (see kgo's cursor.topicID: adopting
// blind is unsafe because OffsetForLeaderEpoch carries no topic ID). So a
// topic that is deleted and recreated stalls with UNKNOWN_TOPIC_ID
// forever. franz-go has an escape hatch for this — it purges a topic that
// has been ABSENT from metadata for 15s — but that only fires while the
// client can still see metadata. If the broker itself is away for the
// whole delete/recreate window (a crash that loses its data, then
// warmup.sh recreating the topics), the client never observes the gap, so
// it wakes to a new ID it will not take, and that pair or exchange
// silently disappears from the UI until someone restarts the container.
// That is exactly what happened on 2026-09-08.
//
// Purging the topic ourselves is the same escape hatch, triggered by the
// error instead of by absence: it drops the stale cursor, and because we
// subscribe by regex the topic is re-discovered on the next metadata
// update with its current ID.
func (c *Consumer) handleFetchErr(topic string, partition int32, err error) {
	log.Printf("kafka[%s]: fetch error %s[%d]: %v", c.name, topic, partition, err)
	if !c.shouldPurge(topic, err, time.Now()) {
		return
	}
	// Deliberately does NOT quote the recovery line's wording. An earlier
	// version spelled out "a 'first record from <topic>' line means it
	// recovered", and the e2e test — which greps the logs for exactly that —
	// matched THIS line instead and passed against a build with the fix
	// removed. A log line that quotes another log line is a trap for anything
	// reading the log.
	log.Printf("kafka[%s]: %s was recreated with a new topic ID — purging it so the regex re-discovers it; "+
		"it announces itself again once a record arrives", c.name, topic)
	// PurgeTopicsFromClient issues a BLOCKING metadata call, so it must
	// never run on the poll loop — that would stall consumption for every
	// other topic this client serves.
	go func() {
		c.client.PurgeTopicsFromClient(topic)
		// The purge only drops the caches. Nothing then asks for this topic
		// again until the metadata max age elapses (5 minutes by default),
		// because the purge also removed the cursor whose fetch errors were
		// driving the refreshes. Without this the fix still works but takes
		// minutes; with it, seconds.
		c.client.ForceMetadataRefresh()
	}()
}

// shouldPurge decides whether this error means the topic must be purged,
// and records the decision. Split out from handleFetchErr so the rule is
// testable without a broker: everything around it is a franz-go call.
func (c *Consumer) shouldPurge(topic string, err error, now time.Time) bool {
	if !errors.Is(err, kerr.UnknownTopicID) {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if last, seen := c.purged[topic]; seen && now.Sub(last) < purgeCooldown {
		return false
	}
	c.purged[topic] = now
	// Forget that we ever saw this topic, so the "first record from ..."
	// line prints again when it recovers. That line is the confirmation
	// that the purge worked.
	delete(c.perTopic, topic)
	return true
}

// count tallies one record and announces a topic the first time it is
// seen. That first line is the answer to "is my regex matching anything?"
// — a subscription that matches nothing is otherwise completely silent,
// and these regexes are matched against the topics that exist at
// subscribe time, so a topic created later never appears at all.
func (c *Consumer) count(rec *kgo.Record) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records++
	c.bytes += int64(len(rec.Value))
	if _, seen := c.perTopic[rec.Topic]; !seen {
		log.Printf("kafka[%s]: first record from %s (partition %d, offset %d)",
			c.name, rec.Topic, rec.Partition, rec.Offset)
	}
	c.perTopic[rec.Topic]++
}

// logStats reports the consumption rate on a ticker, and says so plainly
// when the rate is zero: silence in a log is ambiguous, an explicit "no
// records" line is not.
func (c *Consumer) logStats(ctx context.Context) {
	t := time.NewTicker(statsPeriod)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.mu.Lock()
			total, delta, bytes := c.records, c.records-c.lastCount, c.bytes
			c.lastCount = c.records
			topics := make([]string, 0, len(c.perTopic))
			for topic := range c.perTopic {
				topics = append(topics, topic)
			}
			c.mu.Unlock()

			if delta == 0 {
				log.Printf("kafka[%s]: NO records in the last %s — %d topic(s) subscribed, %d record(s) since start",
					c.name, statsPeriod, len(topics), total)
				continue
			}
			sort.Strings(topics)
			log.Printf("kafka[%s]: %.1f record(s)/s over %d topic(s) [%s] — %d since start, %d KiB",
				c.name, float64(delta)/statsPeriod.Seconds(), len(topics),
				strings.Join(topics, " "), total, bytes/1024)
		}
	}
}
