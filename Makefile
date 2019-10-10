.PHONY: all

all: test cleanup

test:
	go run ./cmd/simpletest/main.go

cleanup:
	rm -rf ./leveldb
