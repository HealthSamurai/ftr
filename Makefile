.PHONY: test build alias

test:
	clojure -M:kaocha:test

build:
	clojure -T:build uber

up:
	docker compose up -d --wait db

down:
	docker-compose down
