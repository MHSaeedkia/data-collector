// Command latency-monitor tails one Kafka topic and prints, for every record,
// where its time went: how long each of jobs 1–6 held it, how long it waited in
// Kafka between them, and how the whole journey compares against the exchange's
// own clock and the broker's write time.
//
//	latency-monitor ex1-p1-orderbook-snapshot-flink
//	latency-monitor --strict-order p1-asks
//
// It replaces scripts/watch-topic.sh, which could print the record timestamp and
// the offset but not the payload — the timings live inside the Avro value, and
// decoding that in shell was not going to happen.
//
// Timestamps are printed in the HOST's timezone (override with TZ=...), not the
// containers'. "Kafka write time" throughout means the record's metadata
// timestamp, never a field in the payload.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"orderbook-latency/internal/config"
	"orderbook-latency/internal/consume"
	"orderbook-latency/internal/event"
	"orderbook-latency/internal/latency"
)

// Exit code 2 for an out-of-order offset under --strict-order, matching the
// scripts/watch-topic.sh this replaces, so anything wrapping it still works.
const exitOutOfOrder = 2

func main() {
	strictOrder := flag.Bool("strict-order", false,
		"stop with exit 2 the moment an offset does not increase")
	envFile := flag.String("env", ".env",
		"path to the .env file holding KAFKA_BOOTSTRAP and SCHEMA_REGISTRY_URL")
	// These two default to EMPTY on purpose: the real defaults live in
	// internal/config, and the .env file has not been read yet at the moment
	// flag defaults are evaluated. An empty flag means "whatever config says".
	brokers := flag.String("brokers", "",
		"kafka bootstrap servers, comma separated (overrides KAFKA_BOOTSTRAP; default localhost:9092)")
	registryURL := flag.String("schema-registry", "",
		"schema registry base url (overrides SCHEMA_REGISTRY_URL; default http://localhost:8082)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [flags] <topic>\n\nflags:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nendpoints are read from %s, then the environment, then those defaults;\n"+
			"a flag beats all three.\n", *envFile)
	}
	flag.Parse()

	if flag.NArg() != 1 || flag.Arg(0) == "" {
		flag.Usage()
		os.Exit(1)
	}
	topic := flag.Arg(0)

	// Precedence, weakest first: built-in default, .env, real environment
	// variable, flag.
	cfg := config.Load(*envFile)
	if *brokers != "" {
		cfg.Bootstrap = *brokers
	}
	if *registryURL != "" {
		cfg.RegistryURL = *registryURL
	}

	// Ctrl-C ends the tail rather than killing it mid-write, so the last block
	// printed is a whole one.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, topic, *strictOrder); err != nil {
		var ooo outOfOrderError
		if errors.As(err, &ooo) {
			fmt.Fprintf(os.Stderr, "ERROR: offset out of order on %s: %d after %d\n",
				topic, ooo.got, ooo.previous)
			os.Exit(exitOutOfOrder)
		}
		fmt.Fprintf(os.Stderr, "latency-monitor: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config.Config, topic string, strictOrder bool) error {
	decoder := event.NewDecoder(cfg.RegistryURL)
	// Per partition: the topics are single-partition today, but an offset check
	// that assumed that would report a false violation the day one is not.
	lastOffset := make(map[int32]int64)

	fmt.Fprintf(os.Stderr, "tailing %s from the end (kafka %s, registry %s)\n",
		topic, cfg.Bootstrap, cfg.RegistryURL)

	return consume.Tail(ctx, cfg.Bootstrap, topic, func(r consume.Record) error {
		if previous, seen := lastOffset[r.Partition]; seen && r.Offset <= previous {
			if strictOrder {
				return outOfOrderError{previous: previous, got: r.Offset}
			}
			fmt.Fprintf(os.Stderr, "WARNING: offset out of order on %s: %d after %d\n",
				topic, r.Offset, previous)
		}
		lastOffset[r.Partition] = r.Offset

		rec, recordName, err := decoder.Decode(r.Value)
		if err != nil {
			// One unreadable record must not end the tail: a topic can carry a
			// record written by a schema this build has never seen.
			fmt.Fprintf(os.Stderr, "skipping offset %d: %v\n", r.Offset, err)
			return nil
		}

		meta := latency.Meta{
			Topic:      topic,
			Partition:  r.Partition,
			Offset:     r.Offset,
			WriteTime:  r.Timestamp,
			RecordName: recordName,
		}
		latency.Render(os.Stdout, meta, rec, latency.Compute(rec, meta.WriteTime))
		return nil
	})
}

type outOfOrderError struct {
	previous int64
	got      int64
}

func (e outOfOrderError) Error() string {
	return fmt.Sprintf("offset out of order: %d after %d", e.got, e.previous)
}
