GOCACHE ?= /tmp/pulsekv-go-cache

.PHONY: fmt test race vet build clean

fmt:
	test -z "$$(gofmt -l .)"

test:
	GOCACHE=$(GOCACHE) go test ./...

race:
	GOCACHE=$(GOCACHE) go test -race ./...

vet:
	GOCACHE=$(GOCACHE) go vet ./...

build:
	mkdir -p bin
	GOCACHE=$(GOCACHE) go build -o bin/pulsekv ./cmd/pulsekv

clean:
	rm -rf bin
