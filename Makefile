refresh:
	-git pull origin
	docker compose -f docker-compose-orderbook-job.yml down -v
	docker compose -f docker-compose-orderbook-job.yml up --build -d
	./scripts/warmup.sh
	cd ./flink/orderbook-job && ./run-job.sh

refresh-consolidator:
	-git pull origin
	docker compose -f docker-compose-orderbook-consolidator.yml down -v
	docker compose -f docker-compose-orderbook-consolidator.yml up --build -d
	./scripts/warmup.sh
	cd ./flink/orderbook-consolidator && ./run-job.sh

# Full raw pipeline: the 6 normalizer jobs plus the consolidator that consumes their output,
# all on the one Flink cluster in docker-compose-normalizer.yml.
# Jobs are submitted DOWNSTREAM-FIRST because every source reads from `latest`: a job started
# after its upstream would miss whatever the upstream produced in between.
refresh-normalizer:
	-git pull origin
	docker compose -f docker-compose-normalizer.yml down -v
	docker compose -f docker-compose-normalizer.yml up --build -d
	./scripts/warmup.sh
	cd ./flink/orderbook-consolidator && ./run-job.sh
	cd ./flink/normalizer && ./run-job.sh job-level-emitter
	cd ./flink/normalizer && ./run-job.sh job-book-builder
	cd ./flink/normalizer && ./run-job.sh job-precision
	cd ./flink/normalizer && ./run-job.sh job-rebaser
	cd ./flink/normalizer && ./run-job.sh job-type-validator
	cd ./flink/normalizer && ./run-job.sh job-pair-extractor

run-normalizer-jobs:
	-git pull origin
	cd ./flink/orderbook-consolidator && ./run-job.sh
	cd ./flink/normalizer && ./run-job.sh job-level-emitter
	cd ./flink/normalizer && ./run-job.sh job-book-builder
	cd ./flink/normalizer && ./run-job.sh job-precision
	cd ./flink/normalizer && ./run-job.sh job-rebaser
	cd ./flink/normalizer && ./run-job.sh job-type-validator
	cd ./flink/normalizer && ./run-job.sh job-pair-extractor