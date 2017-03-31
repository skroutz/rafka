.PHONY: install build test lint vet fmt bench clean

TESTCMD=go test -v

install: test vet fmt
	go install

build: test vet fmt
	go build

test:
	$(TESTCMD)

lint:
	golint

vet:
	go vet

fmt:
	! gofmt -d -e -s . 2>&1 | tee /dev/tty | read

clean:
	go clean
