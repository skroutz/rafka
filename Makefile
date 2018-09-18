.PHONY: install dep build test teste2e testunit lint fmt clean run-rafka testunit-local teste2e-local

default: fmt install test

install:
	go install -v

dep:
	dep ensure -v

build: fmt
	go build -v

testunit-local:
	go test -race

teste2e-local:
	cd test && bundle install --frozen && ./end-to-end -v

lint:
	golint

fmt:
	test -z `go fmt 2>&1`

clean:
	go clean

run-rafka:
	docker-compose -f test/docker-compose.yml up --no-start --build
	docker-compose -f test/docker-compose.yml start
	docker-compose -f test/docker-compose.yml exec rafka make dep build

testunit: run-rafka
	docker-compose -f test/docker-compose.yml exec rafka make testunit-local

teste2e: run-rafka
	docker-compose -f test/docker-compose.yml exec rafka make teste2e-local

test: run-rafka
	docker-compose -f test/docker-compose.yml exec rafka make testunit-local teste2e-local
