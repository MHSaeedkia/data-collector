package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type Exchange struct {
	ID   int
	Name string
}

type Pair struct {
	ID    int
	Base  string
	Quote string
}

var (
	exchanges = []Exchange{
		{ID: 1, Name: "nobitex"},
		{ID: 2, Name: "bitpin"},
	}
	pairs = []Pair{
		{ID: 2, Base: "BTC", Quote: "USDT"},
	}
	sides = []string{"asks", "bids"}
)

// Level mirrors the {"price":"...","quantity":"..."} entries in the original script.
type Level struct {
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
}

// OrderBookEvent mirrors the JSON payload built with `jq` in the original script.
// Identity is pair_id/exchange_id; base/quote/exchange_name are display-only.
type OrderBookEvent struct {
	ExchangeID   int     `json:"exchange_id"`
	ExchangeName string  `json:"exchange_name"`
	Base         string  `json:"base"`
	Quote        string  `json:"quote"`
	PairID       int     `json:"pair_id"`
	Side         string  `json:"side"`
	Type         string  `json:"type"`
	EventTime    int64   `json:"event_time"`
	Levels       []Level `json:"levels"`
}

// pairParams returns the generation params for a pair, mirroring the bash
// pair_params() function: base, step, price decimals, qty min, qty max, qty decimals.
func pairParams(pair string) (base, step float64, priceDecimals int, qtyMin, qtyMax float64, qtyDecimals int) {
	switch pair {
	case "BTC-USDT":
		return 100000, 20, 2, 0.01, 2.5, 4
	default:
		return 100, 1, 2, 1, 100, 2
	}
}

// genLevels generates a randomized levels slice for a (pair, side), mirroring the
// bash gen_levels() awk block.
func genLevels(pair Pair, side string) []Level {
	base, step, priceDecimals, qtyMin, qtyMax, qtyDecimals := pairParams(pair.Base + "-" + pair.Quote)

	n := 3 + rand.Intn(5) // 3..7 levels
	mid := base + (rand.Float64()*10-5)*step

	levels := make([]Level, 0, n)
	for i := 0; i < n; i++ {
		off := float64(i+1) * step * (1 + rand.Float64())
		var price float64
		if side == "asks" {
			price = mid + off
		} else {
			price = mid - off
		}
		qty := qtyMin + rand.Float64()*(qtyMax-qtyMin)

		levels = append(levels, Level{
			Price:    strconv.FormatFloat(price, 'f', priceDecimals, 64),
			Quantity: strconv.FormatFloat(qty, 'f', qtyDecimals, 64),
		})
	}
	return levels
}

// topicName builds the ID-based input topic name: "{side}-p{pair_id}-ex{exchange_id}".
func topicName(side string, pairID, exchangeID int) string {
	return fmt.Sprintf("%s-p%d-ex%d", side, pairID, exchangeID)
}

// ensureTopics creates every {pair}-{side}-{exchange} topic (1 partition, replication
// factor 1), ignoring "already exists" errors so it's safe to call on every run — just
// like the bash version's `kafka-topics --create --if-not-exists`.
func ensureTopics(bootstrap string) error {
	conn, err := kafka.Dial("tcp", bootstrap)
	if err != nil {
		return fmt.Errorf("dial kafka at %s: %w", bootstrap, err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("find controller: %w", err)
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer controllerConn.Close()

	var configs []kafka.TopicConfig
	for _, pair := range pairs {
		for _, side := range sides {
			for _, exchange := range exchanges {
				configs = append(configs, kafka.TopicConfig{
					Topic:             topicName(side, pair.ID, exchange.ID),
					NumPartitions:     1,
					ReplicationFactor: 1,
				})
			}
		}
	}

	if err := controllerConn.CreateTopics(configs...); err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
		return fmt.Errorf("create topics: %w", err)
	}
	return nil
}

// emit publishes one snapshot to {side}-p{pair_id}-ex{exchange_id}, mirroring the bash emit().
func emit(ctx context.Context, writer *kafka.Writer, pair Pair, side string, exchange Exchange, levels []Level) error {
	event := OrderBookEvent{
		ExchangeID:   exchange.ID,
		ExchangeName: exchange.Name,
		Base:         pair.Base,
		Quote:        pair.Quote,
		PairID:       pair.ID,
		Side:         side,
		Type:         "snapshot",
		EventTime:    time.Now().UnixMilli(),
		Levels:       levels,
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	topicName := topicName(side, pair.ID, exchange.ID)
	return writer.WriteMessages(ctx, kafka.Message{
		Topic: topicName,
		Value: body,
	})
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	bootstrap := envOrDefault("KAFKA_BOOTSTRAP", "localhost:9092")

	durationSeconds, err := strconv.Atoi(envOrDefault("DURATION_SECONDS", "600"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid DURATION_SECONDS: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Ensuring topics exist...")
	if err := ensureTopics(bootstrap); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(bootstrap),
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
	}
	defer writer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	count := 0
	end := time.Now().Add(time.Duration(durationSeconds) * time.Second)

	fmt.Printf("Streaming randomized snapshots for %ds (Ctrl-C to stop)...\n", durationSeconds)

loop:
	for time.Now().Before(end) {
		select {
		case <-ctx.Done():
			break loop
		default:
		}

		pair := pairs[rand.Intn(len(pairs))]
		side := sides[rand.Intn(len(sides))]
		exchange := exchanges[rand.Intn(len(exchanges))]

		levels := genLevels(pair, side)
		if err := emit(ctx, writer, pair, side, exchange, levels); err != nil {
			fmt.Printf("\n  emit failed, continuing: %v\n", err)
		}

		count++
		fmt.Printf("\r  sent %-6d (last: %-9s %-4s %-8s)   ", count, pair.Base+"-"+pair.Quote, side, exchange.Name)

		// Pause on ~1 of every 3 steps to create visible bursts and gaps.
		if rand.Intn(3) == 0 {
			sleepFor := time.Duration((1+rand.Float64()*2)*1000) * time.Millisecond
			select {
			case <-ctx.Done():
				break loop
			case <-time.After(sleepFor):
			}
		}
	}

	fmt.Printf("\nDone. Sent %d snapshots over %ds.\n", count, durationSeconds)
}
