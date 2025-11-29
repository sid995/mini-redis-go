# Makefile for mini-redis-go
# Provides common targets for building, testing, and running the project

# Variables
MODULE := github.com/sid995/mini-redis
BINARY_NAME := mini-redis
BINARY_PATH := ./bin/$(BINARY_NAME)
MAIN_PACKAGE := ./cmd/server
TEST_PACKAGES := ./tests/... ./internal/...
BENCHMARK_PACKAGES := ./benchmark/...
GO := go
GOFMT := gofmt
GOVET := go vet

# Build flags
LDFLAGS := -w -s
BUILD_FLAGS := -ldflags "$(LDFLAGS)"

# Default target
.DEFAULT_GOAL := help

.PHONY: help
help: ## Display this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the binary
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p bin
	$(GO) build $(BUILD_FLAGS) -o $(BINARY_PATH) $(MAIN_PACKAGE)
	@echo "Binary built at $(BINARY_PATH)"

.PHONY: run
run: ## Run the application
	$(GO) run $(MAIN_PACKAGE)

.PHONY: install
install: ## Install dependencies
	@echo "Installing dependencies..."
	$(GO) mod download
	$(GO) mod tidy

.PHONY: test
test: ## Run all tests
	@echo "Running tests..."
	$(GO) test -v -race -coverprofile=coverage.out $(TEST_PACKAGES)

.PHONY: test-short
test-short: ## Run tests in short mode
	@echo "Running tests (short mode)..."
	$(GO) test -short -v $(TEST_PACKAGES)

.PHONY: test-coverage
test-coverage: test ## Run tests with coverage report
	@echo "Generating coverage report..."
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

.PHONY: bench
bench: ## Run benchmarks
	@echo "Running benchmarks..."
	$(GO) test -bench=. -benchmem $(BENCHMARK_PACKAGES)

.PHONY: bench-cpu
bench-cpu: ## Run benchmarks with CPU profile
	@echo "Running benchmarks with CPU profiling..."
	$(GO) test -bench=. -benchmem -cpuprofile=cpu.prof $(BENCHMARK_PACKAGES)

.PHONY: fmt
fmt: ## Format all Go code
	@echo "Formatting code..."
	$(GOFMT) -s -w .
	@echo "Code formatted"

.PHONY: fmt-check
fmt-check: ## Check if code is formatted correctly
	@echo "Checking code formatting..."
	@if [ $$($(GOFMT) -l . | wc -l) -ne 0 ]; then \
		echo "Code is not formatted. Run 'make fmt' to fix."; \
		$(GOFMT) -l .; \
		exit 1; \
	fi
	@echo "Code is properly formatted"

.PHONY: vet
vet: ## Run go vet
	@echo "Running go vet..."
	$(GOVET) ./...
	@echo "go vet completed"

.PHONY: lint
lint: fmt-check vet ## Run all linters (fmt-check and vet)

.PHONY: clean
clean: ## Clean build artifacts
	@echo "Cleaning build artifacts..."
	rm -rf bin/
	rm -f coverage.out coverage.html
	rm -f *.prof
	$(GO) clean -cache
	@echo "Clean complete"

.PHONY: mod-tidy
mod-tidy: ## Tidy go.mod file
	@echo "Tidying go.mod..."
	$(GO) mod tidy
	@echo "go.mod tidied"

.PHONY: mod-verify
mod-verify: ## Verify dependencies
	@echo "Verifying dependencies..."
	$(GO) mod verify
	@echo "Dependencies verified"

.PHONY: all
all: clean install fmt vet test build ## Run clean, install, fmt, vet, test, and build

