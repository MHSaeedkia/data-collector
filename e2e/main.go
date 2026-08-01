package main

import (
	"context"
	"log"
	"os"

	"orderbook-e2e/config"
	"orderbook-e2e/scenario"
	"orderbook-e2e/stack"
)

// scenarios are the cases in numbered order. Each one warms the pipeline up for
// its own exchange before it runs, so they are independent and the list can be
// cut down while working on a single case.
var scenarios = []struct {
	name string
	s    scenario.Scenario
}{
	// Nobitex
	{"01-ex1-rest-then-ws-resync", scenario.Ex1RestThenWsResync},
	{"02-ex1-update-before-snapshot", scenario.Ex1UpdateBeforeSnapshot},
	{"03-ex1-sequence-gap", scenario.Ex1SequenceGap},
	{"04-ex1-noise-frames", scenario.Ex1NoiseFrames},
	{"05-ex1-stale-rest-replay", scenario.Ex1StaleRestReplay},
	{"06-ex1-precision-dust", scenario.Ex1PrecisionDust},
	{"07-ex1-rebase-toman", scenario.Ex1RebaseToman},
	{"08-ex1-rebase-scaled-unit", scenario.Ex1RebaseScaledUnit},

	// Bitpin
	{"09-ex2-rest-then-ws-resync", scenario.Ex2RestThenWsResync},
	{"10-ex2-update-before-snapshot", scenario.Ex2UpdateBeforeSnapshot},
	{"11-ex2-sequence-gap", scenario.Ex2SequenceGap},
	{"12-ex2-noise-frames", scenario.Ex2NoiseFrames},
	{"13-ex2-stale-rest-replay", scenario.Ex2StaleRestReplay},
	{"14-ex2-precision-dust", scenario.Ex2PrecisionDust},

	// Wallex
	{"15-ex3-wallex-half-book", scenario.Ex3WallexHalfBook},
	{"16-ex3-empty-side-wipe", scenario.Ex3EmptySideWipe},
	{"17-ex3-precision-dust", scenario.Ex3PrecisionDust},
	{"18-ex3-noise-frames", scenario.Ex3NoiseFrames},
	{"19-ex3-stale-replay", scenario.Ex3StaleReplay},

	// {"01-ex8-update-before-snapshot", scenario.Ex8UpdateBeforeSnapshot},
	// {"02-ex8-happy-path", scenario.Ex8HappyPath},
	// {"03-ex8-sequence-gap", scenario.Ex8SequenceGap},
	// {"04-ex8-stale-duplicate", scenario.Ex8StaleDuplicate},
	// {"05-ex8-precision-dust", scenario.Ex8PrecisionDust},
	// {"06-ex8-noise-frames", scenario.Ex8NoiseFrames},
}

func main() {
	cfg, err := config.Load(".env")
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	if err := stack.Provision(ctx, cfg.ComposeFile); err != nil {
		log.Fatal(err)
	}

	// One failure does not stop the run: a suite this slow is only worth waiting
	// on if it reports every case it can, so failures are collected and listed
	// at the end.
	var failed []string
	for i, sc := range scenarios {
		log.Printf("=== %d/%d %s", i+1, len(scenarios), sc.name)
		if err := scenario.Run(ctx, cfg, sc.s); err != nil {
			log.Printf("FAIL %s: %v", sc.name, err)
			failed = append(failed, sc.name)
		} else {
			log.Printf("PASS %s", sc.name)
		}
		log.Println("=================================")
	}

	if len(failed) > 0 {
		log.Printf("%d/%d scenarios failed: %v", len(failed), len(scenarios), failed)
		os.Exit(1)
	}
	log.Printf("all %d scenarios passed", len(scenarios))
}
