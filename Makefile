all: bin/batchq.linux_amd64

SOURCES := $(shell find . -name '*.go')

bin/batchq.macos_amd64: go.mod go.sum $(SOURCES)
	CGO_ENABLED=1 GOOS=darwin GOARCH=amd64 go build -o bin/batchq.macos_amd64 main.go

bin/batchq.macos_arm64: go.mod go.sum $(SOURCES)
	CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 go build -o bin/batchq.macos_arm64 main.go

bin/batchq.linux_amd64: go.mod go.sum $(SOURCES)
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o bin/batchq.linux_amd64 main.go

clean:
	rm bin/*

run:
	CGO_ENABLED=1 go run main.go

.PHONY: run clean
