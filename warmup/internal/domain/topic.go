package domain

// Topic is a Kafka topic warmup wants to exist, with the retention it should
// carry.
type Topic struct {
	Name        string
	RetentionMS string
}
