# Ptah Migration Library Makefile

.PHONY: help build test integration-test integration-test-sqlserver db-start-sqlserver clean docker-build lint lint-qtlint lint-nolintguard lint-golangci lint-fix install-hooks conformance

VERSION ?= $(shell git describe --tags --always --dirty)
COMMIT ?= $(shell git rev-parse --short HEAD)
DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -X go.5x5.cz/ptah/internal/buildinfo.Version=$(VERSION) \
	-X go.5x5.cz/ptah/internal/buildinfo.Commit=$(COMMIT) \
	-X go.5x5.cz/ptah/internal/buildinfo.Date=$(DATE)

# Default target
help:
	@echo "Ptah Migration Library"
	@echo "====================="
	@echo ""
	@echo "Available targets:"
	@echo "  build              Build all binaries"
	@echo "  test               Run unit tests"
	@echo "  integration-test   Run integration tests using Docker Compose"
	@echo "  integration-test-sqlserver"
	@echo "                     Run SQL Server opt-in integration smoke tests"
	@echo "  lint               Run golangci-lint, qtlint, and nolintguard"
	@echo "  conformance        Show Atlas conformance scoreboard location"
	@echo "  lint-fix           Run auto-fixable linters"
	@echo "  install-hooks      Install local Git hooks"
	@echo "  docker-build       Build Docker images"
	@echo "  clean              Clean build artifacts"
	@echo "  help               Show this help message"

# Build all binaries
build:
	@echo "Building Ptah binaries..."
	go build -ldflags "$(LDFLAGS)" -o bin/ptah ./cmd/ptah
	go build -ldflags "$(LDFLAGS)" -o bin/ptah-ls ./cmd/ptah-ls
	go build -ldflags "$(LDFLAGS)" -o bin/ptah-compat ./cmd/ptah-compat
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

integration-test-sqlserver: docker-build db-start-sqlserver
	@echo "Running integration tests against SQL Server only..."
	docker compose --profile test --profile sqlserver run --rm ptah-tester \
		--databases=sqlserver \
		--scenarios=apply_incremental_migrations,rollback_migrations,upgrade_to_specific_version,check_current_version,read_actual_db_schema,dry_run_support,operation_planning,failure_diagnostics,idempotency_reapply,idempotency_up_to_date,parallel_migrate_smoke,cleanup_support,dynamic_sqlserver_identity_schema_bracket_reserved_words \
		--report=html --verbose

# Run integration tests using Docker Compose with custom arguments
integration-test-custom: docker-build
	@echo "Running integration tests with custom arguments..."
	@echo "Usage: make integration-test-custom ARGS='--report=json --databases=postgres'"
	docker compose --profile test run --rm ptah-tester $(ARGS)

# Start databases only (for development)
db-start:
	@echo "Starting databases..."
	docker compose up -d postgres mysql mariadb cockroachdb yugabytedb

db-start-sqlserver:
	@echo "Starting SQL Server..."
	docker compose --profile sqlserver up -d --wait sqlserver

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

# GO_MODULES is every Go module tracked in this repository, discovered rather
# than listed. A tool that names its modules by hand covers a module because
# somebody remembered it, and stops covering the next one without saying so.
# That is what happened here: nolintguard named all three modules by hand, and
# golangci-lint never left the root module at all. See
# scripts/list-go-modules.sh.
GO_MODULES := $(shell scripts/list-go-modules.sh)

# Lint code
lint: lint-qtlint lint-nolintguard lint-golangci

lint-nolintguard:
	@echo "Running nolintguard..."
	@nolintguard="$$(go tool -n nolintguard)"; \
	for module in $(GO_MODULES); do \
		(cd "$$module" && go vet -vettool="$$nolintguard" -require-justification ./...) || exit 1; \
		(cd "$$module" && go vet -tags=integration -vettool="$$nolintguard" -require-justification ./...) || exit 1; \
	done

lint-golangci:
	@echo "Running golangci-lint..."
	@for module in $(GO_MODULES); do \
		(cd "$$module" && golangci-lint run ./...) || exit 1; \
	done

# Both invocations are required, and neither is redundant.
#
# -multi-module discovers the modules under the directory operands, so testkit/
# and examples/orm-loaders/gorm/ are covered from here. `go list` otherwise
# resolves patterns against the module holding the working directory, and
# ./testkit/... answers "directory prefix testkit does not contain main module
# or its selected dependencies".
#
# The two contours are two different builds, not a subset and a superset:
# satisfying `integration` also drops every file a constraint excludes from
# that contour.
#
# lint-qtlint-fix runs the rules in separate passes. Applied together with
# -fix they can collide: one rule deletes a receiver declaration the other
# rule's rewrite still references. See go-extras/qtlint#65.
#
# -require-testing-run joined this set once both contours reached zero: 27
# untagged and 139 tagged reports at the start, none now. Most were rewritten
# by the rule itself; the rest were tables holding a func field typed
# func(c *qt.C, ...), which is the shape that leaves the rule unable to bound
# what a subtest closure reaches, and they carry data now.
#
# Nineteen were converted by hand after the rule withheld its own fix. The
# withholding is right in general -- a *qt.C handed to a field could reach
# (*qt.C).Defer, and a rewrite that stops a deferred function running does not
# fail, it just stops cleaning up. It does not apply here, and that is checked
# rather than assumed: no test in this repository calls Defer or Done at all.
# If one ever does, this rule reports its subtest and the fix is withheld
# again, which is the outcome to want.
QTLINT_RULES := -require-qt-c-receiver -require-data-rows -require-testing-run

lint-qtlint:
	@echo "Running qtlint..."
	go tool qtlint -multi-module $(QTLINT_RULES) ./...
	go tool qtlint -multi-module $(QTLINT_RULES) -tags integration ./...

lint-qtlint-fix:
	@for rule in $(QTLINT_RULES); do \
		go tool qtlint -multi-module $$rule -fix ./... || exit 1; \
		go tool qtlint -multi-module $$rule -fix -tags integration ./... || exit 1; \
	done

lint-fix:
	@echo "Running auto-fixable linters..."
	$(MAKE) lint-qtlint-fix
	golangci-lint run --fix ./...
	$(MAKE) lint

# Atlas parity scoreboard. The executable harness and Apache-2.0 Atlas fixture
# corpus intentionally live outside this MIT repository, under
# stokaro/ptah-atlas-conformance. Keep the dependency direction one-way:
# conformance imports Ptah; Ptah does not import or vendor conformance.
conformance:
	@echo "Atlas conformance scoreboard:"
	@echo "  docs/conformance.md"
	@echo "  https://github.com/stokaro/ptah-atlas-conformance"
	@echo ""
	@echo "Run the harness from that repository:"
	@echo "  make probe        # offline corpus report"
	@echo "  make budget       # offline regression budget"
	@echo "  make gate         # full offline parity gate"
	@echo "  make probe-live   # live DB round-trip report"
	@echo "  make budget-live  # live DB regression budget"
	@echo "  make probe-diff   # Atlas CE differential report"
	@echo "  make budget-diff  # Atlas CE differential regression budget"

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
