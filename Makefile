all: bin/batchq.linux

SOURCES := $(shell find . -name '*.go')

bin/batchq.macos_amd64: go.mod go.sum $(SOURCES)
	CGO_ENABLED=1 GOOS=darwin GOARCH=amd64 go build -o bin/batchq.macos_amd64 main.go

bin/batchq.macos_arm64: go.mod go.sum $(SOURCES)
	CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 go build -o bin/batchq.macos_arm64 main.go

bin/batchq.linux_musl_amd64: go.mod go.sum $(SOURCES)
	CC=x86_64-linux-musl-gcc \
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -ldflags='-extldflags "-static"' -o bin/batchq.linux_musl_amd64 main.go

bin/batchq.linux_musl_aarch64: go.mod go.sum $(SOURCES)
	CC=aarch64-linux-musl-gcc \
	CGO_ENABLED=1 GOOS=linux GOARCH=arm64 go build -ldflags='-extldflags "-static"' -o bin/batchq.linux_musl_aarch64 main.go

#bin/batchq.linux_arm64: go.mod go.sum $(SOURCES)
#	CC=aarch64-linux-gnu-gcc \
#	CGO_ENABLED=1 GOOS=linux GOARCH=arm64 go build -o bin/batchq.linux_arm64 main.go

bin/batchq.linux: go.mod go.sum $(SOURCES)
	CGO_ENABLED=1 GOOS=linux go build -o bin/batchq.linux main.go


clean:
	rm bin/*

run:
	CGO_ENABLED=1 go run main.go

.PHONY: run clean
