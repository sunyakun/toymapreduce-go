.PHONY: build mrapps

build:
	mkdir -p output/bin
	go build -race -o output/bin/mapreduce cmd/main.go

mrapps:
	mkdir -p ouitput/bin
	go build -race -buildmode plugin -o output/bin/wc.so example/mrapps/wc/wc.go
	go build -race -buildmode plugin -o output/bin/crash.so example/mrapps/crash/crash.go
	go build -race -buildmode plugin -o output/bin/nocrash.so example/mrapps/nocrash/nocrash.go
	go build -race -buildmode plugin -o output/bin/mtiming.so example/mrapps/mtiming/mtiming.go
	go build -race -buildmode plugin -o output/bin/rtiming.so example/mrapps/rtiming/rtiming.go
	go build -race -buildmode plugin -o output/bin/early_exit.so example/mrapps/early_exit/early_exit.go
	go build -race -buildmode plugin -o output/bin/jobcount.so example/mrapps/jobcount/jobcount.go
	go build -race -buildmode plugin -o output/bin/indexer.so example/mrapps/indexer/indexer.go

	go build -race --o output/bin/mrsequential example/mrsequential/mrsequential.go