# batchq is pure Go (modernc.org/sqlite); no CGO, no C cross-toolchain.
# CGO_ENABLED=0 makes Go produce a statically linked binary even without
# musl, which is what we want for portable distribution.

CGO_ENABLED ?= 0
export CGO_ENABLED

SOURCES := $(shell find . -name '*.go')

# Version resolution: delegated to scripts/version.sh. See that script
# for the rules; the short version is "exact tag → tag name; else
# next-patch-dev-<sha>; else fallback". Override on the command line
# (VERSION=foo make ...) if you need a specific string.
VERSION ?= $(shell ./scripts/version.sh)

LDFLAGS := -ldflags "-X github.com/mbreese/batchq/cmd.Version=$(VERSION)"

# Default target is just the host binary — fast iteration during dev.
# `make all` cross-builds every supported target; that's what CI runs
# and what release tarballs are built from.
HOST_OS := $(shell go env GOOS)
HOST_ARCH := $(shell go env GOARCH)
HOST_BINARY := bin/batchq.$(HOST_OS)_$(HOST_ARCH)

.DEFAULT_GOAL := $(HOST_BINARY)

all: \
	bin/batchq.linux_amd64 \
	bin/batchq.linux_arm64 \
	bin/batchq.macos_amd64 \
	bin/batchq.macos_arm64

bin/batchq.macos_amd64: go.mod go.sum $(SOURCES)
	GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o $@ main.go

bin/batchq.macos_arm64: go.mod go.sum $(SOURCES)
	GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -o $@ main.go

bin/batchq.linux_amd64: go.mod go.sum $(SOURCES)
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $@ main.go

bin/batchq.linux_arm64: go.mod go.sum $(SOURCES)
	GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o $@ main.go

clean:
	rm -f bin/*

run:
	go run $(LDFLAGS) main.go

test:
	go test ./...

.PHONY: all clean run test
