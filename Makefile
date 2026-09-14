.PHONY: build test test-integration benchmark benchmark-unit benchmark-integration fmt

build:
	go build -trimpath -ldflags "-s -w -X kbsync/internal/cli.version=$${VERSION:-dev}" -o bin/kbsync ./

test:
	go test -race ./...

test-integration:
	go test -count=1 -race -tags=integration -timeout=10m ./...

benchmark: benchmark-unit benchmark-integration

benchmark-unit:
	go test -run '^$$' -bench 'Benchmark(ScanBufferLifecycle|CheckpointPersistence|CheckpointExtraction)$$' -benchmem -benchtime=3x -count=3 ./internal/syncer

benchmark-integration:
	go test -run '^$$' -tags=integration -bench '^BenchmarkKingbase' -benchmem -benchtime=3x -count=3 -timeout=30m ./internal/syncer

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './third_party/*')
