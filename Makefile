# batchq is pure Go (modernc.org/sqlite); no CGO, no C cross-toolchain.
# CGO_ENABLED=0 makes Go produce a statically linked binary even without
# musl, which is what we want for portable distribution.

CGO_ENABLED ?= 0
export CGO_ENABLED

SOURCES := $(shell find . -name '*.go')

# Version resolution: exact-tag commits get the tag name (e.g. v0.2.0);
# anything else gets a "v0.2.0-dev-<short-sha>" string. Bump the dev
# base ("v0.2.0") here when starting work on the next minor series.
VERSION ?= $(shell \
	if t=$$(git describe --tags --exact-match 2>/dev/null); then \
		echo "$$t"; \
	elif s=$$(git rev-parse --short HEAD 2>/dev/null); then \
		echo "v0.2.0-dev-$$s"; \
	else \
		echo "dev"; \
	fi)

LDFLAGS := -ldflags "-X github.com/mbreese/batchq/cmd.Version=$(VERSION)"

all: bin/batchq.linux

bin/batchq.macos_amd64: go.mod go.sum $(SOURCES)
	GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o $@ main.go

bin/batchq.macos_arm64: go.mod go.sum $(SOURCES)
	GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -o $@ main.go

bin/batchq.linux_amd64: go.mod go.sum $(SOURCES)
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $@ main.go

bin/batchq.linux_arm64: go.mod go.sum $(SOURCES)
	GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o $@ main.go

bin/batchq.linux: go.mod go.sum $(SOURCES)
	GOOS=linux go build $(LDFLAGS) -o $@ main.go

clean:
	rm -f bin/*

run:
	go run $(LDFLAGS) main.go

test:
	go test ./...

.PHONY: all clean run test
