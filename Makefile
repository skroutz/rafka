.PHONY: install dep build test teste2e lint fmt clean runrafka dockertest

install: fmt test
	go install -v

dep:
	dep ensure -v

build: fmt
	go build -v

test:
	go test -race

teste2e:
	cd test && bundle install --frozen && ./end-to-end -v

lint:
	golint

fmt:
	test -z `go fmt 2>&1 | tee /dev/tty`

clean:
	go clean


runrafka:
	./rafka -k test/kafka.test.json

dockertest:
	docker-compose -f test/docker-compose.yml up --build --no-start
	docker-compose -f test/docker-compose.yml start
	docker-compose -f test/docker-compose.yml ps
	docker-compose -f test/docker-compose.yml exec rafka make dep build test teste2e
