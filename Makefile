.PHONY: test build alias

test:
	clojure -M:dev:kaocha

build:
	clj -T:build uber
