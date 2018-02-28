.PHONY: install build test lint vet fmt clean list

install: vet fmt test
	go install

build: vet fmt test
	go build

test:
	go test -race
	test/end-to-end -v

lint:
	golint

vet:
	go vet

fmt:
	! gofmt -d -e -s *.go 2>&1 | tee /dev/tty | read

clean:
	go clean

CWD=$(shell pwd)
spawn:
	docker run -p 6380:6380 -v $(CWD):/rafka --rm --name rafka_server_1 --network kafkaclustertestbed_default skroutz/rafka
