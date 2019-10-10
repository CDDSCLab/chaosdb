.PHONY: all

all: test clean_leveldbData

test:
	go run ./cmd/simpletest/main.go

clean_leveldbData:
	rm -rf ./leveldb
