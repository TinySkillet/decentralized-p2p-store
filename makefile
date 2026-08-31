DEFAULT_TARGET: build

.PHONY: build run test test-race test-race-libp2p vet fmt check clean

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

# The node suite again over the other transport. The suite default is tcp;
# both must stay green, because which one a network runs is a deployment
# choice.
test-race-libp2p:
	@P2PSTORAGE_TEST_TRANSPORT=libp2p go test ./node/ -race -timeout 900s

vet:
	@go vet ./...

# Kept identical to the check CI runs.
fmt:
	@gofmt -l .

check: fmt vet test-race test-race-libp2p

clean:
	@rm -rf bin/
