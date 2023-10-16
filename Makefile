.PHONY: test build alias

init:
	git submodule update --recursive && make up

test:
	clojure -M:kaocha:test:pipeline

build:
	clojure -T:build uber

up:
	docker compose up --detach --wait db

down:
	docker compose down --volumes
