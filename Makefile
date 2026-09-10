.PHONY: all check build test vet fmt vendor lint clean

GO ?= go

all: check

check: fmt test vet build

build:
	$(GO) build ./...

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

fmt:
	@test -z "$$($(GO)fmt -l .)" || { echo 'gofmt required'; $(GO)fmt -l .; exit 1; }

vendor:
	$(GO) mod vendor

lint:
	@if command -v golangci-lint >/dev/null 2>&1; then golangci-lint run ./...; else echo 'golangci-lint not installed; skipped'; fi

clean:
	$(GO) clean ./...
