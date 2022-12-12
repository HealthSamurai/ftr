.PHONY: test build alias

test:
	clojure -M:kaocha:test

build:
	clojure -T:build uber

up:
	docker-compose up -d db

down:
	docker-compose down
