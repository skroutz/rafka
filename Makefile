.PHONY: install dep build test teste2e testunit lint fmt clean run-rafka run-rafka-local testunit-local teste2e-local

default: fmt build

install:
	go install -v

build: fmt
	GOARCH=amd64 GOOS=linux CGO_ENABLED=1 go build -v -ldflags '-w -s -X main.VersionSuffix=$(shell git rev-parse HEAD)' -trimpath

testunit-local:
	go test -race

teste2e-local:
	cd test && bundle install --frozen && ./end-to-end -v

fmt:
	@if [ -z "$(shell gofmt -l . | grep -v vendor/)" ]; then \
		echo "Source code needs re-formatting! Use 'go fmt' manually."; \
		false; \
	fi

clean:
	rm -rf vendor/
	go clean

run-rafka-local: build
	./rafka -c test/librdkafka.test.json

run-rafka: dep
	docker-compose -f test/docker-compose.yml up --no-start --build
	docker-compose -f test/docker-compose.yml start

testunit: run-rafka
	docker-compose -f test/docker-compose.yml exec rafka make testunit-local

teste2e: run-rafka
	docker-compose -f test/docker-compose.yml exec rafka make teste2e-local

test: run-rafka
	docker-compose -f test/docker-compose.yml exec rafka make testunit-local teste2e-local
