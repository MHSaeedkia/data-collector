// Package server exposes the harness over HTTP: a scenario is posted as JSON
// instead of being compiled in as a package-level var, and the response says
// whether the pipeline produced what the scenario wanted.
package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	httpSwagger "github.com/swaggo/http-swagger/v2"
	"github.com/swaggo/swag"

	"orderbook-e2e/config"
	_ "orderbook-e2e/docs" // the generated spec registers itself on import
	"orderbook-e2e/events"
	"orderbook-e2e/scenario"
)

// RunRequest is a scenario plus a name to report it under. The scenario is
// embedded, so the body is the scenario's own fields at the top level.
type RunRequest struct {
	Name string `json:"name" example:"ex3-half-book"`
	scenario.Scenario
}

// RunResponse is the result of one run. A scenario that fails its assertions is
// a 200 with status "failed" — the run itself worked, the pipeline disagreed.
type RunResponse struct {
	Name       string `json:"name,omitempty" example:"ex3-half-book"`
	Status     string `json:"status" enums:"ok,failed" example:"ok"`
	Error      string `json:"error,omitempty"`
	DurationMS int64  `json:"duration_ms" example:"92310"`
}

// runner holds what a run needs and serializes them. Every run recreates the
// exchange's topics and resubmits the Flink jobs, so two at once would tear
// down each other's pipeline mid-scenario.
type runner struct {
	cfg config.Config
	mu  sync.Mutex
}

// New builds the router. The stack is expected to be provisioned already —
// the caller does that once at startup, not per request.
func New(cfg config.Config) http.Handler {
	rn := &runner{cfg: cfg}

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Post("/scenarios/run", rn.run)

	// Swagger UI at /swagger/ — the spec and the UI assets are both compiled in,
	// so the page works on a machine with no network. doc.json is served ahead of
	// the wildcard (chi matches the static route first) so the spec can carry the
	// worked example; everything else falls through to the UI.
	spec := specJSON()
	r.Get("/swagger/doc.json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(spec))
	})
	r.Get("/swagger/*", httpSwagger.WrapHandler)
	return r
}

// exampleRequest is the body Swagger UI prefills "Try it out" with: the real
// Ex1PrecisionDust case, so the page opens on data that runs and passes rather
// than on a skeleton someone has to fill in.
func exampleRequest() RunRequest {
	return RunRequest{Name: "ex1-precision-dust", Scenario: scenario.Ex1PrecisionDust}
}

// specJSON is the generated spec with that example attached to server.RunRequest.
// swag has no annotation for a schema-level example — `sources` is two multi-line
// raw JSON documents, which a field `example` tag cannot express — so it is
// injected here from the compiled-in scenario, which is the same value the
// built-in run uses and so cannot drift from it. The example goes on the
// definition rather than beside the body parameter's `$ref`, because a sibling of
// `$ref` is dropped when the spec is resolved.
//
// Any failure here is a spec problem, not a run problem: log it and serve what
// swag generated.
func specJSON() string {
	doc, err := swag.ReadDoc()
	if err != nil {
		log.Printf("swagger: read spec: %v", err)
		return "{}"
	}

	var spec map[string]any
	if err := json.Unmarshal([]byte(doc), &spec); err != nil {
		log.Printf("swagger: parse spec: %v", err)
		return doc
	}

	definitions, _ := spec["definitions"].(map[string]any)
	runRequest, ok := definitions["server.RunRequest"].(map[string]any)
	if !ok {
		log.Printf("swagger: no server.RunRequest definition to put the example on")
		return doc
	}
	runRequest["example"] = exampleRequest()

	out, err := json.Marshal(spec)
	if err != nil {
		log.Printf("swagger: re-encode spec: %v", err)
		return doc
	}
	return string(out)
}

// run executes one scenario against the pipeline.
//
//	@Summary		Run one scenario
//	@Description	Warms the pipeline up for the scenario's exchange/pair, produces every `sources` entry to `ex{exchange_id}-raw`, then reads the snapshot, rejected and aggregated topics back and compares them to the `want_*` streams.
//	@Description
//	@Description	Job 6 is always asserted: `want_aggregated` is the book the pair's `p{pair_id}-asks` / `p{pair_id}-bids` topics must END on, and every level carries the `exchange_id` it came from because the aggregator unions across exchanges instead of summing them. Only the final record on each side is read, not the whole stream. Omitting `want_aggregated` does NOT skip the check — the expectation is then derived from the last `want_snapshots` entry with the scenario's exchange stamped on every level, which is exact only because a scenario feeds ONE exchange. Send it explicitly to say what you mean.
//	@Description
//	@Description	A scenario that does not match is **200 with `"status":"failed"`**, not a 5xx: the run itself worked, the pipeline disagreed. A non-200 always means the scenario never ran.
//	@Description
//	@Description	Runs are serialized — each one cancels the Flink jobs and recreates the exchange's topics, so a request arriving while another is in flight gets 409 instead of being queued. Expect a successful call to take minutes.
//	@Description
//	@Description	"Try it out" is prefilled with the real `ex1-precision-dust` case — a nobitex REST snapshot plus a WS delta on ex1/pair 1, where two bid prices merge once truncated to the market's 2 places and a dust quantity truncates to zero and deletes its level. Send it as-is and it should come back `"status":"ok"`.
//	@Tags			scenarios
//	@Accept			json
//	@Produce		json
//	@Param			request	body		RunRequest	true	"The scenario to run. Unknown fields are rejected, so a mistyped want_snapshots cannot silently pass."
//	@Success		200		{object}	RunResponse	"The scenario ran; status says whether it matched"
//	@Failure		400		{object}	RunResponse	"Body could not be decoded or failed validation"
//	@Failure		409		{object}	RunResponse	"Another scenario is already running"
//	@Router			/scenarios/run [post]
func (rn *runner) run(w http.ResponseWriter, r *http.Request) {
	var req RunRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid body: %v", err))
		return
	}
	if err := validate(req.Scenario); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// A queued run would sit behind a warmup and a 60s read for no benefit, so a
	// second caller is told to come back rather than made to wait.
	if !rn.mu.TryLock() {
		writeError(w, http.StatusConflict, "a scenario is already running")
		return
	}
	defer rn.mu.Unlock()

	log.Printf("=== run %s (ex%d/p%d, %d sources)", req.Name, req.ExchangeID, req.PairID, len(req.Sources))
	start := time.Now()
	err := scenario.Run(r.Context(), rn.cfg, req.Scenario)
	res := RunResponse{Name: req.Name, Status: "ok", DurationMS: time.Since(start).Milliseconds()}
	if err != nil {
		res.Status = "failed"
		res.Error = err.Error()
		log.Printf("FAIL %s: %v", req.Name, err)
	} else {
		log.Printf("PASS %s", req.Name)
	}
	writeJSON(w, http.StatusOK, res)
}

func validate(s scenario.Scenario) error {
	if s.ExchangeID <= 0 {
		return fmt.Errorf("exchange_id must be positive")
	}
	if s.PairID <= 0 {
		return fmt.Errorf("pair_id must be positive")
	}
	if len(s.Sources) == 0 {
		return fmt.Errorf("sources must not be empty")
	}
	// A run takes minutes, so a want_aggregated that cannot possibly match is
	// worth catching here rather than in the diff at the end of it. The levels
	// are the aggregated form, not the snapshot form: an untagged level is the
	// mistake a caller makes when they copy a want_snapshots side across.
	if s.WantAggregated != nil {
		for _, side := range []struct {
			name   string
			levels []events.AggregatedLevel
		}{
			{"asks", s.WantAggregated.Asks},
			{"bids", s.WantAggregated.Bids},
		} {
			for i, level := range side.levels {
				if level.ExchangeID <= 0 {
					return fmt.Errorf("want_aggregated.%s[%d]: exchange_id must be positive — aggregated levels carry the exchange they came from", side.name, i)
				}
				if level.Price == "" || level.Quantity == "" {
					return fmt.Errorf("want_aggregated.%s[%d]: price and quantity must not be empty", side.name, i)
				}
			}
		}
	}
	return nil
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, RunResponse{Status: "failed", Error: msg})
}

func writeJSON(w http.ResponseWriter, code int, body RunResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("write response: %v", err)
	}
}
