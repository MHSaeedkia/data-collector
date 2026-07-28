package domain

// JobModules are the six normalizer job modules, named exactly as their
// directories under flink/normalizer (which is also how Dockerfile.jobs names
// the jars it produces).
//
// The order is downstream-first, matching manual-test-data/reset.sh: a
// downstream job is running and subscribed before the job feeding it can emit
// anything. Sources read from latest(), so the reverse order would race.
var JobModules = []string{
	"job-aggregator",
	"job-book-builder",
	"job-precision",
	"job-rebaser",
	"job-type-validator",
	"job-pair-extractor",
}

// SchemaFiles maps each Schema Registry subject to its .avsc file under
// schemas/ at the repo root. Ported from scripts/warmup.sh.
var SchemaFiles = []struct {
	Subject string
	File    string
}{
	{"aggregated-order-book-event", "aggregated_order_book_event.avsc"},
	{"raw-order-book-event", "raw_order_book_event.avsc"},
	{"order-book-snapshot", "order_book_snapshot.avsc"},
	{"rejected-order-book-event", "rejected_order_book_event.avsc"},
}
