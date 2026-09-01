// Package producer publishes the harness test payloads to Kafka.
package producer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// SendJSON produces doc to topic as a single record. The document is compacted
// first, which also rejects a malformed payload before it reaches the broker.
func SendJSON(ctx context.Context, broker, topic, doc string) error {
	var value bytes.Buffer
	if err := json.Compact(&value, []byte(doc)); err != nil {
		return fmt.Errorf("payload for %s is not valid JSON: %w", topic, err)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(broker, ",")...))
	if err != nil {
		return err
	}
	defer cl.Close()

	rec := &kgo.Record{Topic: topic, Value: value.Bytes()}
	if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", topic, err)
	}

	slog.Debug("produced", "bytes", value.Len(), "topic", topic)
	return nil
}
