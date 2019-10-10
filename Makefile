.PHONY: all

all: test clean

test:
	go run ./cmd/simpletest/main.go

clean:
	rm -rf ./leveldb
