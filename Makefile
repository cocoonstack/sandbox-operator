SHELL := /usr/bin/env bash

BINARY := bin/sandbox-operator
MAIN := ./cmd/sandbox-operator
APISERVER_BINARY := bin/sandbox-apiserver
APISERVER_MAIN := ./cmd/sandbox-apiserver
APISERVER_IMG ?= ghcr.io/cocoonstack/sandbox-apiserver:dev
ENVDPROXY_BINARY := bin/envd-proxy
ENVDPROXY_MAIN := ./cmd/envd-proxy
ENVDPROXY_IMG ?= ghcr.io/cocoonstack/envd-proxy:dev
VERSION_PKG := github.com/cocoonstack/sandbox-operator/internal/version

GIT_VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo unknown)
GIT_SHA ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_DATE ?= $(shell date -u +'%Y-%m-%dT%H:%M:%SZ')
LD_FLAGS := -s -w -X $(VERSION_PKG).gitVersion=$(GIT_VERSION) -X $(VERSION_PKG).gitSHA=$(GIT_SHA) -X $(VERSION_PKG).buildDate=$(BUILD_DATE)

## Build-tagged harnesses under test/, one tag per directory
TAGGED_HARNESSES := e2e e2ebench l2bench l3bench poolbench scalebench scalestress envdproxysmoke

## Target OSes for vet / lint
GOOSES ?= linux darwin

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool versions
GOLANGCILINT_VERSION ?= v2.13.2
GOLANGCILINT_ROOT := $(LOCALBIN)/golangci-lint-$(GOLANGCILINT_VERSION)
GOLANGCILINT := $(GOLANGCILINT_ROOT)/golangci-lint

GOFUMPT_VERSION ?= v0.11.0
GOFUMPT_ROOT := $(LOCALBIN)/gofumpt-$(GOFUMPT_VERSION)
GOFMT := $(GOFUMPT_ROOT)/gofumpt

GOIMPORTS_VERSION ?= v0.49.0
GOIMPORTS_ROOT := $(LOCALBIN)/goimports-$(GOIMPORTS_VERSION)
GOIMPORTS := $(GOIMPORTS_ROOT)/goimports
GOIMPORTS_LOCAL_PREFIXES := github.com/cocoonstack/

.DEFAULT_GOAL := all

## Tool download targets
.PHONY: golangci-lint
golangci-lint: $(GOLANGCILINT)
$(GOLANGCILINT):
	GOBIN=$(GOLANGCILINT_ROOT) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCILINT_VERSION)

.PHONY: gofumpt
gofumpt: $(GOFMT)
$(GOFMT):
	GOBIN=$(GOFUMPT_ROOT) go install mvdan.cc/gofumpt@$(GOFUMPT_VERSION)

.PHONY: goimports
goimports: $(GOIMPORTS)
$(GOIMPORTS):
	GOBIN=$(GOIMPORTS_ROOT) go install golang.org/x/tools/cmd/goimports@$(GOIMPORTS_VERSION)

.PHONY: all
all: fmt-check vet test build

.PHONY: build
build: ## Build the operator binary.
	mkdir -p bin
	go build -ldflags "$(LD_FLAGS)" -o $(BINARY) $(MAIN)

.PHONY: apiserver-build
apiserver-build: ## Build the aggregated sandbox-apiserver binary.
	mkdir -p bin
	go build -ldflags "-s -w" -o $(APISERVER_BINARY) $(APISERVER_MAIN)

.PHONY: apiserver-image
apiserver-image: ## Build the aggregated sandbox-apiserver image (override APISERVER_IMG).
	docker build -f Dockerfile.apiserver -t $(APISERVER_IMG) .

.PHONY: envdproxy-build
envdproxy-build: ## Build the envd-proxy binary.
	mkdir -p bin
	go build -ldflags "-s -w" -o $(ENVDPROXY_BINARY) $(ENVDPROXY_MAIN)

.PHONY: envdproxy-image
envdproxy-image: ## Build the envd-proxy image (override ENVDPROXY_IMG).
	docker build -f Dockerfile.envdproxy -t $(ENVDPROXY_IMG) .

.PHONY: test
test: vet ## Run unit tests.
	go test ./...

.PHONY: test-race
test-race: ## Run unit tests with the race detector.
	go test -race ./...

.PHONY: coverage
coverage: vet ## Write unit-test coverage to bin/coverage.out.
	mkdir -p bin
	go test -coverprofile=bin/coverage.out ./...

.PHONY: vet
vet: ## Run go vet on every target OS.
	@for goos in $(GOOSES); do \
		echo "==> go vet GOOS=$$goos"; \
		GOOS=$$goos go vet ./... || exit 1; \
	done

.PHONY: vet-tagged
vet-tagged: ## Type-check the build-tagged e2e/bench harnesses.
	@for t in $(TAGGED_HARNESSES); do \
		echo "go vet -tags $$t ./test/$$t"; \
		go vet -tags $$t ./test/$$t || exit 1; \
	done

.PHONY: lint
lint: golangci-lint ## Run golangci-lint on every target OS.
	@for goos in $(GOOSES); do \
		echo "==> golangci-lint GOOS=$$goos"; \
		GOOS=$$goos $(GOLANGCILINT) run ./... || exit 1; \
	done

.PHONY: fmt
fmt: gofumpt goimports ## Format code with gofumpt and goimports.
	$(GOFMT) -l -w .
	$(GOIMPORTS) -l -w --local '$(GOIMPORTS_LOCAL_PREFIXES)' .

.PHONY: fmt-check
fmt-check: gofumpt goimports ## Check formatting (fails if files need formatting).
	@test -z "$$($(GOFMT) -l .)" || { echo "Files need formatting (gofumpt):"; $(GOFMT) -l .; exit 1; }
	@test -z "$$($(GOIMPORTS) -l .)" || { echo "Files need formatting (goimports):"; $(GOIMPORTS) -l .; exit 1; }

.PHONY: generate
generate: ## Regenerate CRDs, deep copies, and RBAC.
	go mod download -modfile=tools.mod
	go generate ./...

.PHONY: deps
deps: ## Download module dependencies.
	go mod download
	go mod download -modfile=tools.mod

.PHONY: clean
clean: ## Remove build and coverage outputs.
	rm -rf bin
	go clean -testcache

.PHONY: cloc
cloc: ## Count lines of code excluding tests (requires cloc).
	cloc --exclude-dir=vendor,dist,bin --exclude-ext=json --not-match-f='_test\.go$$' .

.PHONY: help
help: ## Show available targets.
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: api-docs
api-docs: ## Regenerate docs/api.md from the API types
	GOWORK=off go run github.com/elastic/crd-ref-docs@v0.2.0 --config=hack/crd-ref-docs.yaml --source-path=. --renderer=markdown --output-path=docs/api.md --max-depth=12
