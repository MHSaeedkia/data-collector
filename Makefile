FLINK_RUN := ./flink/run-job.sh

# Jobs are listed DOWNSTREAM-FIRST and must be submitted in this order: every source reads from
# `latest`, so a job started after its upstream would miss whatever the upstream produced in between.
NORMALIZER_JOBS := job-aggregator job-book-builder job-precision job-rebaser job-type-validator job-pair-extractor job-parser
# merger sits downstream of job-aggregator (it reads p{id}-{side}), so it goes before the whole chain.
# adjustment and merger both read job 6's p{id}-{side}, so both are downstream of it and go first.
ALL_JOBS := adjustment merger $(NORMALIZER_JOBS)

# Per-job parallelism: PARALLELISM_<job>, falling back to DEFAULT_PARALLELISM. Every job needs that
# many task slots, so the total must fit taskmanager.numberOfTaskSlots (50 in docker-compose.yml,
# 4 x 4 = 16 in docker-compose.prod.yml — the 9 jobs need 15 of those, so prod has ONE spare slot).
# Override from the command line, e.g. `make run-all-jobs PARALLELISM_merger=3`.
DEFAULT_PARALLELISM := 1
# Sized from live Flink metrics on 2026-09-13, against ~1200 books/s out of job-book-builder:
# - job-aggregator was 100% busy at ~1000/s; after the sort/serializer fix a single subtask is
#   estimated at ~1350/s, too close to the input, so 2.
# - merger was ~40% busy per subtask at 5 (~480/s each), so 4 runs at ~60%.
# - adjustment was 100% busy at 1 (~530/s), so 3 runs at ~75%.
PARALLELISM_job-aggregator := 2
PARALLELISM_merger := 4
PARALLELISM_adjustment := 3

# $(call submit_jobs,<jobs>) submits each job in the order given, stopping at the first failure.
submit_jobs = $(foreach job,$(1),PARALLELISM=$(or $(PARALLELISM_$(job)),$(DEFAULT_PARALLELISM)) $(FLINK_RUN) $(job) || exit 1;)

# Avro schemas + Kafka topics. warmup/ is a Go project, replacing the old scripts/warmup.sh: the
# shell version spawned a JVM per topic through `docker exec kafka kafka-topics` and took about an
# hour at ~3000 topics, where the admin API takes them in batches. Settings come from warmup/.env,
# created from warmup/.env.example on the first run. Re-running is safe, and it rewrites
# retention.ms on topics that already exist when .env no longer matches.
warmup:
	@$(MAKE) -C warmup run

# Tail one topic and print where each record's time went — per-job, the waits between
# jobs, and the totals against the exchange clock and the Kafka write time. Replaces
# scripts/watch-topic.sh, which could not read the payload the timings live in.
#   make watch TOPIC=p1-asks
watch:
	@$(MAKE) -C latency-monitor run TOPIC=$(TOPIC)

# Full raw pipeline: the 6 upstream normalizer jobs plus the terminal aggregator that unions their
# per-exchange books, all on the one Flink cluster in docker-compose.yml.
#
# ⚠ `warmup` MUST have been run for the CURRENT schemas and topics before these submit anything.
# Both this target and prod-deploy do it; run-normalizer-jobs and run-all-jobs do NOT.
refresh-normalizer:
	-git pull origin
	docker compose -f docker-compose.yml down -v
	docker compose -f docker-compose.yml up --build -d
	@$(MAKE) --no-print-directory warmup
	@$(call submit_jobs,$(NORMALIZER_JOBS))

run-normalizer-jobs:
	-git pull origin
	./scripts/cancel-flink-jobs.sh
	@$(call submit_jobs,$(NORMALIZER_JOBS))

# Everything on the cluster: the normalizer pipeline plus flink/merger's summed view.
run-all-jobs:
	-git pull origin
	./scripts/cancel-flink-jobs.sh
	@$(call submit_jobs,$(ALL_JOBS))

# ---------------------------------------------------------------------------
# Production — docker-compose.prod.yml
#
# Deliberately different from the dev targets above, in three ways:
#   1. NO `down -v`. On a dev box that wipes some test data; on a prod box it deletes the Kafka
#      log dirs and the Postgres data dir. There is no prod target that removes a volume.
#   2. NO `git pull`. Check out the ref you intend to run, then deploy it — a target that pulls
#      whatever moved on the branch leaves nothing to roll back to. (Building on the box at all
#      is still a known hazard; building images in CI is the real fix and is not done yet.)
#   3. `--build` is REQUIRED, not optional: flink/normalizer/Dockerfile pre-creates and chowns
#      /opt/flink/ha and /opt/flink/archive, and without it HA silently has nowhere to write.
PROD_COMPOSE := docker compose -f docker-compose.prod.yml

prod-up:
	$(PROD_COMPOSE) up -d --build

# Full deploy: bring the stack up, seed topics/schemas, then submit all 9 jobs downstream-first.
# Cancels first — HA (M3) resubmits the previous job graphs on its own, so submitting on top of a
# recovered cluster would leave two of everything running.
prod-deploy: prod-up
	@$(MAKE) --no-print-directory warmup
	./scripts/cancel-flink-jobs.sh
	@$(call submit_jobs,$(ALL_JOBS))
	@$(MAKE) --no-print-directory prod-verify

# run-job.sh exits as soon as one job reports RUNNING and never rechecks. This asserts the whole
# set is up before the deploy claims success, instead of leaving it to M9's 5-minute alert.
prod-verify:
	@n=$$(curl -s http://localhost:7070/jobs | jq '[.jobs[] | select(.status == "RUNNING")] | length'); \
	 expected=$$(echo $(ALL_JOBS) | wc -w | tr -d ' '); \
	 if [ "$$n" != "$$expected" ]; then \
	     echo "ERROR: $$n jobs RUNNING, expected $$expected"; exit 1; \
	 fi; \
	 echo "OK: $$n/$$expected jobs RUNNING"

prod-logs:
	$(PROD_COMPOSE) logs -f --tail=200

.PHONY: warmup refresh-normalizer run-normalizer-jobs run-all-jobs prod-up prod-deploy prod-verify prod-logs
