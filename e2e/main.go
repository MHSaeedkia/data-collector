package main

import (
	"context"
	"log"
	"os"

	"orderbook-e2e/config"
	"orderbook-e2e/scenario"
)

// scenarios are the manual-test-data cases in their directory order. Each one
// warms the pipeline up for its own exchange before it runs, so they are
// independent and the list can be cut down while working on a single case.
var scenarios = []struct {
	name string
	s    scenario.Scenario
}{
	{"01-ex8-update-before-snapshot", scenario.Ex8UpdateBeforeSnapshot},
	{"02-ex8-happy-path", scenario.Ex8HappyPath},
	{"03-ex8-sequence-gap", scenario.Ex8SequenceGap},
	{"04-ex8-stale-duplicate", scenario.Ex8StaleDuplicate},
	{"05-ex8-precision-dust", scenario.Ex8PrecisionDust},
	{"06-ex8-noise-frames", scenario.Ex8NoiseFrames},
	{"07-ex3-wallex-half-book", scenario.Ex3WallexHalfBook},
	{"08-ex1-rest-then-ws-resync", scenario.Ex1RestThenWsResync},
	{"09-ex1-update-before-snapshot", scenario.Ex1UpdateBeforeSnapshot},
	{"10-ex1-sequence-gap", scenario.Ex1SequenceGap},
	{"11-ex1-noise-frames", scenario.Ex1NoiseFrames},
	{"12-ex1-stale-rest-replay", scenario.Ex1StaleRestReplay},
	{"13-ex2-rest-then-ws-resync", scenario.Ex2RestThenWsResync},
	{"14-ex2-update-before-snapshot", scenario.Ex2UpdateBeforeSnapshot},
	{"15-ex2-sequence-gap", scenario.Ex2SequenceGap},
	{"16-ex2-noise-frames", scenario.Ex2NoiseFrames},
	{"17-ex2-stale-rest-replay", scenario.Ex2StaleRestReplay},
}

func main() {
	cfg, err := config.Load(".env")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// if err := stack.Provision(ctx, cfg.ComposeFile); err != nil {
	// 	log.Fatal(err)
	// }

	// One failure does not stop the run: a suite this slow is only worth waiting
	// on if it reports every case it can, so failures are collected and listed
	// at the end.
	var failed []string
	for i, sc := range scenarios {
		log.Printf("=== %d/%d %s", i+1, len(scenarios), sc.name)
		if err := scenario.Run(ctx, cfg, sc.s); err != nil {
			log.Printf("FAIL %s: %v", sc.name, err)
			failed = append(failed, sc.name)
			continue
		}
		log.Printf("PASS %s", sc.name)
	}

	if len(failed) > 0 {
		log.Printf("%d/%d scenarios failed: %v", len(failed), len(scenarios), failed)
		os.Exit(1)
	}
	log.Printf("all %d scenarios passed", len(scenarios))
}
