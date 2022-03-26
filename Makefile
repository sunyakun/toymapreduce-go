.PHONY: build

build:
	mkdir -p output/bin
	go build -race -o output/bin/mapreduce cmd/main.go