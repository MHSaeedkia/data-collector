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
