// Package domain holds the harness's own vocabulary: plain values with no
// dependency on Kafka, Docker, HTTP or Flink. Everything here is pure and
// unit-testable with nothing running.
package domain

// Scope is the one exchange/market combination a scenario exercises. Every
// topic the scenario needs is derived from it, which is why warmup does not
// have to consult the markets database the way scripts/warmup.sh does.
type Scope struct {
	ExchangeID int
	PairID     int
}

// Endpoints are the addresses of a running stack, as reachable from the test
// process. They are only known after the containers start — nothing may
// hardcode them.
type Endpoints struct {
	KafkaBroker       string
	SchemaRegistryURL string
	FlinkAPI          string
}

// Topic is a Kafka topic to create. Every topic in this pipeline is
// single-partition, so partition 0 is always the whole topic.
type Topic struct {
	Name        string
	RetentionMS int64
}

// Subject is an Avro schema registered under a Schema Registry subject. The
// pipeline binds one canonical subject per message shape rather than one per
// topic, so the subject names do not follow Confluent's TopicNameStrategy.
type Subject struct {
	Name   string
	Schema string
}

// Jar is a built Flink job jar held in memory, ready to upload.
type Jar struct {
	Name  string
	Bytes []byte
}
