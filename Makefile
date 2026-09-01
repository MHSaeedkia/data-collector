FLINK_RUN := ./flink/run-job.sh

# Jobs are listed DOWNSTREAM-FIRST and must be submitted in this order: every source reads from
# `latest`, so a job started after its upstream would miss whatever the upstream produced in between.
NORMALIZER_JOBS := job-aggregator job-book-builder job-precision job-rebaser job-type-validator job-pair-extractor
# merger sits downstream of job-aggregator (it reads p{id}-{side}), so it goes before the whole chain.
# adjustment and merger both read job 6's p{id}-{side}, so both are downstream of it and go first.
ALL_JOBS := adjustment merger $(NORMALIZER_JOBS)

# Full raw pipeline: the 5 upstream normalizer jobs plus the terminal aggregator that unions their
# per-exchange books, all on the one Flink cluster in docker-compose.yml.
refresh-normalizer:
	-git pull origin
	docker compose -f docker-compose.yml down -v
	docker compose -f docker-compose.yml up --build -d
	./scripts/warmup.sh
	@for job in $(NORMALIZER_JOBS); do $(FLINK_RUN) $$job || exit 1; done

run-normalizer-jobs:
	-git pull origin
	./scripts/cancel-flink-jobs.sh
	@for job in $(NORMALIZER_JOBS); do $(FLINK_RUN) $$job || exit 1; done

# Everything on the cluster: the normalizer pipeline plus flink/merger's summed view.
run-all-jobs:
	-git pull origin
	./scripts/cancel-flink-jobs.sh
	@for job in $(ALL_JOBS); do $(FLINK_RUN) $$job || exit 1; done

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

# Full deploy: bring the stack up, seed topics/schemas, then submit all 8 jobs downstream-first.
# Cancels first — HA (M3) resubmits the previous job graphs on its own, so submitting on top of a
# recovered cluster would leave two of everything running.
prod-deploy: prod-up
	./scripts/warmup.sh
	./scripts/cancel-flink-jobs.sh
	@for job in $(ALL_JOBS); do $(FLINK_RUN) $$job || exit 1; done
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

.PHONY: refresh-normalizer run-normalizer-jobs run-all-jobs prod-up prod-deploy prod-verify prod-logs
