refresh:
	git pull origin
	docker compose -f docker-compose.yml down -v
	docker compose -f docker-compose.yml up --build -d