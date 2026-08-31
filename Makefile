# Full raw pipeline: the 5 upstream normalizer jobs plus the terminal aggregator that unions their
# per-exchange books, all on the one Flink cluster in docker-compose.yml.
# Jobs are submitted DOWNSTREAM-FIRST because every source reads from `latest`: a job started
# after its upstream would miss whatever the upstream produced in between.
refresh-normalizer:
	-git pull origin
	docker compose -f docker-compose.yml down -v
	docker compose -f docker-compose.yml up --build -d
	./scripts/warmup.sh
	cd ./flink/normalizer && ./run-job.sh job-aggregator
	cd ./flink/normalizer && ./run-job.sh job-book-builder
	cd ./flink/normalizer && ./run-job.sh job-precision
	cd ./flink/normalizer && ./run-job.sh job-rebaser
	cd ./flink/normalizer && ./run-job.sh job-type-validator
	cd ./flink/normalizer && ./run-job.sh job-pair-extractor

run-normalizer-jobs:
	-git pull origin
	./scripts/cancel-flink-jobs.sh
	cd ./flink/normalizer && ./run-job.sh job-aggregator
	cd ./flink/normalizer && ./run-job.sh job-book-builder
	cd ./flink/normalizer && ./run-job.sh job-precision
	cd ./flink/normalizer && ./run-job.sh job-rebaser
	cd ./flink/normalizer && ./run-job.sh job-type-validator
	cd ./flink/normalizer && ./run-job.sh job-pair-extractor
