# batchq is pure Go (modernc.org/sqlite); no CGO, no C cross-toolchain.
# CGO_ENABLED=0 makes Go produce a statically linked binary even without
# musl, which is what we want for portable distribution.

CGO_ENABLED ?= 0
export CGO_ENABLED

SOURCES := $(shell find . -name '*.go')

all: bin/batchq.linux

bin/batchq.macos_amd64: go.mod go.sum $(SOURCES)
	GOOS=darwin GOARCH=amd64 go build -o $@ main.go

bin/batchq.macos_arm64: go.mod go.sum $(SOURCES)
	GOOS=darwin GOARCH=arm64 go build -o $@ main.go

bin/batchq.linux_amd64: go.mod go.sum $(SOURCES)
	GOOS=linux GOARCH=amd64 go build -o $@ main.go

bin/batchq.linux_arm64: go.mod go.sum $(SOURCES)
	GOOS=linux GOARCH=arm64 go build -o $@ main.go

bin/batchq.linux: go.mod go.sum $(SOURCES)
	GOOS=linux go build -o $@ main.go

clean:
	rm -f bin/*

run:
	go run main.go

test:
	go test ./...

.PHONY: all clean run test
