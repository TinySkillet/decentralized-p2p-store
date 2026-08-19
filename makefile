DEFAULT_TARGET: build

.PHONY: build run test test-race vet fmt check clean

build:
	@go build -o bin/p2p

run:
	@./bin/p2p

# The unit suite. Multi-node tests bring real nodes up on loopback ports, so
# allow more than the default 10 minute timeout on a slow machine.
test:
	@go test ./... -timeout 300s

# The concurrency fixes are only meaningful if this stays clean.
test-race:
	@go test ./... -race -timeout 900s

vet:
	@go vet ./...

# Excludes vendor/: gofmt recurses into directories, and the vendored
# dependencies are third-party code we do not reformat. Kept identical to the
# check CI runs.
fmt:
	@gofmt -l $(shell find . -name '*.go' -not -path './vendor/*')

check: fmt vet test-race

clean:
	@rm -rf bin/
