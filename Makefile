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