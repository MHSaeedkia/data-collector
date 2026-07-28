// Package warmup brings the stack to a known state before a scenario runs.
package warmup

import (
	"context"

	"orderbook-e2e/config"
	"orderbook-e2e/flink"
	"orderbook-e2e/topics"
)

// Run puts the pipeline for one exchange/pair back to a clean start: it stops
// the Flink jobs, wipes and recreates the topics, then submits the jobs again.
//
// Jobs go down first because a running job holds the topics it consumes open
// while they are being deleted, and one left alive would attach to the
// recreated topics mid-setup and read the provisioning as pipeline input.
func Run(ctx context.Context, cfg config.Config, exchangeID, pairID int64) error {
	if err := flink.CancelJobs(ctx, cfg.FlinkAPI); err != nil {
		return err
	}

	if err := topics.Delete(ctx, cfg.KafkaBroker, exchangeID, pairID); err != nil {
		return err
	}

	if err := topics.Create(ctx, cfg.KafkaBroker, exchangeID, pairID); err != nil {
		return err
	}

	return flink.RunJobs(ctx, cfg.FlinkAPI, cfg.NormalizerDir)
}
