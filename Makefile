# Ptah Migration Library Makefile

.PHONY: help build test integration-test clean docker-build lint lint-qtlint lint-fix install-hooks

# Default target
help:
	@echo "Ptah Migration Library"
	@echo "====================="
	@echo ""
	@echo "Available targets:"
	@echo "  build              Build all binaries"
	@echo "  test               Run unit tests"
	@echo "  integration-test   Run integration tests using Docker Compose"
	@echo "  lint               Run golangci-lint and qtlint"
	@echo "  lint-fix           Run auto-fixable linters"
	@echo "  install-hooks      Install local Git hooks"
	@echo "  docker-build       Build Docker images"
	@echo "  clean              Clean build artifacts"
	@echo "  help               Show this help message"

# Build all binaries
build:
	@echo "Building Ptah binaries..."
	go build -o bin/ptah ./cmd/main.go
	go build -o bin/ptah-integration-test ./cmd/integration-test

# Run unit tests
test:
	@echo "Running unit tests..."
	go test -v ./...

# Build Docker image for integration tests
docker-build:
	@echo "Building Docker image for integration tests..."
	docker compose --profile test build ptah-tester

# Run integration tests using Docker Compose
integration-test: docker-build
	@echo "Starting databases and running integration tests..."
	docker compose --profile test run --rm ptah-tester --report=html --verbose

# Run integration tests with specific format
integration-test-json: docker-build
	@echo "Running integration tests with JSON report..."
	docker compose --profile test run --rm ptah-tester --report=json --verbose

# Run integration tests with text report
integration-test-txt: docker-build
	@echo "Running integration tests with text report..."
	docker compose --profile test run --rm ptah-tester --report=txt --verbose

# Run integration tests with stdout report
integration-test-stdout: docker-build
	@echo "Running integration tests with stdout report..."
	docker compose --profile test run --rm ptah-tester --report=stdout --verbose

# Run specific scenarios
integration-test-basic: docker-build
	@echo "Running basic integration tests..."
	docker compose --profile test run --rm ptah-tester \
		--scenarios=apply_incremental_migrations,rollback_migrations,upgrade_to_specific_version \
		--report=html --verbose

# Run integration tests against specific database
integration-test-postgres: docker-build
	@echo "Running integration tests against PostgreSQL only..."
	docker compose --profile test run --rm ptah-tester --databases=postgres --report=html --verbose

integration-test-mysql: docker-build
	@echo "Running integration tests against MySQL only..."
	docker compose --profile test run --rm ptah-tester --databases=mysql --report=html --verbose

integration-test-mariadb: docker-build
	@echo "Running integration tests against MariaDB only..."
	docker compose --profile test run --rm ptah-tester --databases=mariadb --report=html --verbose

integration-test-cockroachdb: docker-build
	@echo "Running integration tests against CockroachDB only..."
	docker compose --profile test run --rm ptah-tester --databases=cockroachdb --scenarios=dynamic_cockroachdb_common_subset --report=html --verbose

integration-test-yugabytedb: docker-build
	@echo "Running integration tests against YugabyteDB only..."
	docker compose --profile test run --rm ptah-tester --databases=yugabytedb --scenarios=dynamic_yugabytedb_common_subset --report=html --verbose

# Run integration tests using Docker Compose with custom arguments
integration-test-custom: docker-build
	@echo "Running integration tests with custom arguments..."
	@echo "Usage: make integration-test-custom ARGS='--report=json --databases=postgres'"
	docker compose --profile test run --rm ptah-tester $(ARGS)

# Start databases only (for development)
db-start:
	@echo "Starting databases..."
	docker compose up -d postgres mysql mariadb cockroachdb yugabytedb

# Stop databases
db-stop:
	@echo "Stopping databases..."
	docker compose down

# View database logs
db-logs:
	@echo "Showing database logs..."
	docker compose logs -f postgres mysql mariadb cockroachdb yugabytedb

# Clean up Docker resources
docker-clean:
	@echo "Cleaning up Docker resources..."
	docker compose down -v
	docker system prune -f

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf bin/
	rm -rf integration/reports/*
	go clean ./...

# Development helpers
dev-setup: db-start
	@echo "Setting up development environment..."
	@echo "Waiting for databases to be ready..."
	sleep 10
	@echo "Development environment ready!"

# Run a quick smoke test
smoke-test: docker-build
	@echo "Running smoke test..."
	docker compose --profile test run --rm ptah-tester \
		--scenarios=apply_incremental_migrations,check_current_version \
		--databases=postgres --report=txt

# Generate test coverage
coverage:
	@echo "Generating test coverage..."
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Lint code
lint: lint-qtlint
	@echo "Running golangci-lint..."
	golangci-lint run ./...

lint-qtlint:
	@echo "Running qtlint..."
	go tool qtlint ./...

lint-fix:
	@echo "Running auto-fixable linters..."
	go tool qtlint -fix ./...
	golangci-lint run --fix ./...
	$(MAKE) lint

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Install local Git hooks
install-hooks:
	@echo "Installing Git hooks..."
	./scripts/install-hooks.sh

# Run all checks (format, lint, test)
check: fmt lint test
	@echo "All checks passed!"

# Full CI pipeline
ci: check integration-test
	@echo "CI pipeline completed successfully!"

# Show available Docker Compose commands
docker-help:
	@echo "Docker Compose Commands for Integration Tests"
	@echo "============================================="
	@echo ""
	@echo "Basic usage:"
	@echo "  docker compose --profile test run --rm ptah-tester [OPTIONS]"
	@echo ""
	@echo "Examples:"
	@echo "  # Run all tests with HTML report"
	@echo "  docker compose --profile test run --rm ptah-tester --report=html"
	@echo ""
	@echo "  # Run specific scenarios"
	@echo "  docker compose --profile test run --rm ptah-tester --scenarios=apply_incremental_migrations,rollback_migrations"
	@echo ""
	@echo "  # Test specific database"
	@echo "  docker compose --profile test run --rm ptah-tester --databases=postgres"
	@echo ""
	@echo "  # Generate JSON report"
	@echo "  docker compose --profile test run --rm ptah-tester --report=json"
	@echo ""
	@echo "  # Verbose output"
	@echo "  docker compose --profile test run --rm ptah-tester --verbose"
	@echo ""
	@echo "Available options:"
	@echo "  --report FORMAT     Report format: txt, json, html (default: txt)"
	@echo "  --databases DBS     Databases: postgres,mysql,mariadb (default: all)"
	@echo "  --scenarios SCNS    Specific scenarios to run (default: all)"
	@echo "  --verbose           Enable verbose output"
