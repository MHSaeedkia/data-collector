// Package kafka holds the harness's Kafka adapters. This file is the topic
// admin — adapter for ports.TopicCreator.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"orderbook-e2e/internal/domain"
)

// Every topic in this pipeline is single-partition on a single broker, which
// is what makes "read partition 0 from a known offset" a complete read.
const (
	partitions        = 1
	replicationFactor = 1
)

// Admin creates topics on one broker.
type Admin struct {
	broker string
}

// NewAdmin returns an admin for a bootstrap address.
func NewAdmin(broker string) *Admin {
	return &Admin{broker: broker}
}

// Create creates every topic, grouping by retention so each distinct retention
// is one CreateTopics call. Topics that already exist are left alone.
func (a *Admin) Create(ctx context.Context, topics []domain.Topic) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(a.broker))
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)

	byRetention := map[int64][]string{}
	order := []int64{}
	for _, topic := range topics {
		if _, seen := byRetention[topic.RetentionMS]; !seen {
			order = append(order, topic.RetentionMS)
		}
		byRetention[topic.RetentionMS] = append(byRetention[topic.RetentionMS], topic.Name)
	}

	for _, retention := range order {
		value := strconv.FormatInt(retention, 10)
		configs := map[string]*string{"retention.ms": &value}

		resp, err := admin.CreateTopics(ctx, partitions, replicationFactor, configs, byRetention[retention]...)
		if err != nil {
			return fmt.Errorf("create topics: %w", err)
		}
		for _, created := range resp {
			if created.Err != nil && !errors.Is(created.Err, kerr.TopicAlreadyExists) {
				return fmt.Errorf("create topic %s: %w", created.Topic, created.Err)
			}
		}
	}

	return nil
}
