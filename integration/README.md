# Ptah Migration Library Integration Tests

This directory contains comprehensive integration tests for the Ptah migration library. The tests validate migration functionality across PostgreSQL-family targets, MySQL, MariaDB, ClickHouse, and SQL Server.

## Overview

The integration test suite covers all aspects of the migration system as outlined in the integration test plan:

### 🧱 Basic Functionality
- Apply incremental migrations
- Roll back migrations
- Upgrade to specific version
- Check current version
- Generate desired schema
- Read actual DB schema
- Dry-run support
- Operation planning
- Schema diff
- Failure diagnostics

### 🔁 Idempotency
- Re-apply already applied migrations
- Run migrate up when database is already up-to-date

### 🔀 Parallel Execution Smoke
- Launch two migrate up processes in parallel
- Verify at least one runner succeeds and the final migration state is consistent
- Ptah does not yet provide a migration-level lock; enforce a single production runner externally until #124 lands

### 🧪 Partial Failure Recovery
- Handle multi-step migrations with intentional failures
- Validate recovery and rollback capabilities

### ⏱ Timestamp Verification
- Check that `applied_at` timestamps are stored correctly

### 📂 Manual Patch Detection
- Detect manual schema changes via schema diff

### 🔒 Permission Restrictions
- Test behavior with limited database privileges

### 🧹 Cleanup Support
- Drop all tables and re-run from empty state on PostgreSQL, MySQL, MariaDB,
  ClickHouse, and SQL Server

The standalone integration runner reads scenario connections from
`<DATABASE>_URL`. Set the optional matching `<DATABASE>_CLEANUP_URL` when
cleanup requires broader privileges. The scenario and cleanup URLs may use
different credentials, but the runner rejects a cleanup URL that addresses a
different database. Cleanup credentials are exposed only to database reset
operations; migration generation, application, validation, and permission
scenarios continue to use the restricted scenario connection.

For MySQL and MariaDB, keep scenario execution restricted while granting the
cleanup connection the global metadata and destructive privileges listed
below:

```bash
MYSQL_URL="$MYSQL_RESTRICTED_URL" \
MYSQL_CLEANUP_URL="$MYSQL_ADMIN_URL" \
MARIADB_URL="$MARIADB_RESTRICTED_URL" \
MARIADB_CLEANUP_URL="$MARIADB_ADMIN_URL" \
  ./integration-test --databases=mysql,mariadb
```

The database-realm cleanup tests use dedicated scratch databases and verify
that replay cleanup removes database-scoped artifacts without crossing into
another database. Every live Go test is in `integration/` or a subpackage and
uses the file-level `integration` build tag. The contour runner executes the
whole tree serially and fails if no test runs, a result is incomplete, or any
test or subtest skips. Graphviz `dot` must also be installed because the
visualization contour validates generated DOT and SVG with the real renderer.

The authoritative service and environment-variable matrix is maintained in
[the integration workflow](../.github/workflows/go-integration-tests.yml).
Start equivalent services, export every variable from its tagged-integration
step, install Graphviz, and then run the single recursive contour:

```bash
go run ./internal/cmd/testcontour \
  --tags integration \
  --race \
  --timeout 45m
```

Do not run selected test names or individual packages as a CI substitute for
this contour. An integration test is never white-box: integration tests use
`package *_test`; tests that need package-private access are unit tests, not
integration tests.

Each URL must identify an administrative connection intended for tests. The
success-path tests create a separate temporary database and drop it during
cleanup. Protected-name tests connect to the named administrative database
only to prove cleanup fails before mutation.

ClickHouse cleanup requires global `SHOW DATABASES` and `SHOW TABLES`
visibility. ClickHouse does not expose dependency metadata for ordinary views,
so Ptah fails closed when another user database contains a view, materialized
view, live/window view, dictionary, or `Buffer`, `Distributed`, or `Merge`
table. Use a dedicated ClickHouse development server without unrelated
dependency-capable objects.

## Architecture

### Components

- **`framework.go`** - Core test framework with TestRunner and DatabaseHelper
- **`reporter.go`** - Report generation in multiple formats (TXT, JSON, HTML)
- **`scenarios.go`** - Basic test scenarios implementation
- **`scenarios_advanced.go`** - Advanced test scenarios (concurrency, idempotency)
- **`scenarios_misc.go`** - Miscellaneous test scenarios (timestamps, permissions)

### Test Fixtures

- **`fixtures/migrations/basic/`** - PostgreSQL-family standard migration set
- **`fixtures/migrations/basic_mysql/`** - MySQL and MariaDB standard migration set
- **`fixtures/migrations/basic_clickhouse/`** - ClickHouse standard migration set
- **`fixtures/migrations/basic_sqlserver/`** - SQL Server variant of the standard migration set
- **`fixtures/migrations/failing/`** - Migrations with intentional failures
- **`fixtures/migrations/failing_mysql/`** - MySQL and MariaDB intentional failure set
- **`fixtures/migrations/failing_sqlserver/`** - SQL Server variant of the intentional failure set
- **`fixtures/migrations/partial_failure/`** - Multi-step migrations with failures
- **`fixtures/migrations/partial_failure_mysql/`** - MySQL and MariaDB partial failure set
- **`fixtures/migrations/partial_failure_sqlserver/`** - SQL Server variant of the partial failure set
- **`fixtures/entities/`** - Go entity definitions for schema generation tests

## Running Tests

Docker Compose runs the standalone scenario runner with isolated databases. The
tagged Go integration contour instead uses local services matching the
authoritative GitHub Actions workflow and also requires the pinned Atlas CE
oracle and Graphviz.

### Basic Usage

```bash
# Run all tests with default settings (text report)
docker compose --profile test run --rm ptah-tester

# Run with HTML report (recommended for detailed analysis)
docker compose --profile test run --rm ptah-tester --report=html

# Run with JSON report (good for CI/CD integration)
docker compose --profile test run --rm ptah-tester --report=json

# Enable verbose output for debugging
docker compose --profile test run --rm ptah-tester --verbose
```

### Listing Available Scenarios

Before running tests, you can list all available scenarios to see what's available:

```bash
# List all scenarios (both static and dynamic)
docker compose --profile test run --rm ptah-tester list

# List only static scenarios (basic functionality tests)
docker compose --profile test run --rm ptah-tester list --static

# List only dynamic scenarios (versioned entity evolution tests)
docker compose --profile test run --rm ptah-tester list --dynamic

# Show help for the list command
docker compose --profile test run --rm ptah-tester list --help
```

The list command displays:
- 📋 **Static scenarios**: Traditional migration tests using pre-built migration files
- 🔄 **Dynamic scenarios**: Advanced tests using versioned entity fixtures with ✨ indicating enhanced step recording
- Scenario descriptions and usage examples

### Scenario Selection

```bash
# Run specific scenarios only
docker compose --profile test run --rm ptah-tester --scenarios=apply_incremental_migrations,rollback_migrations

# Run basic functionality tests
docker compose --profile test run --rm ptah-tester --scenarios=apply_incremental_migrations,upgrade_to_specific_version,check_current_version

# Run idempotency tests
docker compose --profile test run --rm ptah-tester --scenarios=idempotency_reapply,idempotency_up_to_date

# Run failure recovery tests
docker compose --profile test run --rm ptah-tester --scenarios=failure_diagnostics,partial_failure_recovery

# Run dynamic scenarios for schema evolution testing
docker compose --profile test run --rm ptah-tester --scenarios=dynamic_basic_evolution,dynamic_rollback_single

# Run generator round-trip edge-case fixtures
docker compose --profile test run --rm ptah-tester --scenarios=migration_generator_roundtrip_fixtures
```

The `migration_generator_roundtrip_fixtures` scenario validates generated
migrations through apply, introspect, step-by-step rollback, down-to-zero, and
re-apply cycles. Its fixtures cover empty schemas, single tables, composite
primary keys, foreign-key chains and diamonds, cycles, same-name
`CHECK`/`UNIQUE` changes, enum add/remove transitions, and foreign keys added
to already-existing columns.

### Database Selection

```bash
# Test against PostgreSQL only
docker compose --profile test run --rm ptah-tester --databases=postgres

# Test against MySQL only
docker compose --profile test run --rm ptah-tester --databases=mysql

# Test against MariaDB only
docker compose --profile test run --rm ptah-tester --databases=mariadb

# Test against specific combination
docker compose --profile test run --rm ptah-tester --databases=postgres,mysql

# Test the CockroachDB common-subset scenario when a CockroachDB URL is configured
docker compose --profile test run --rm ptah-tester --databases=cockroachdb --scenarios=dynamic_cockroachdb_common_subset

# Test the YugabyteDB common-subset scenario when a YugabyteDB URL is configured
docker compose --profile test run --rm ptah-tester --databases=yugabytedb --scenarios=dynamic_yugabytedb_common_subset

# Test SQL Server's opt-in migration and schema-rendering subset
make integration-test-sqlserver

# Or run the SQL Server acceptance scenario directly
docker compose --profile sqlserver up -d --wait sqlserver
docker compose --profile test --profile sqlserver run --rm ptah-tester --databases=sqlserver --scenarios=dynamic_sqlserver_identity_schema_bracket_reserved_words
```

### Combined Options

```bash
# Comprehensive test with detailed reporting
docker compose --profile test run --rm ptah-tester --report=html --verbose

# Quick smoke test
docker compose --profile test run --rm ptah-tester --scenarios=apply_incremental_migrations --databases=postgres --report=txt

# CI/CD friendly execution
docker compose --profile test run --rm ptah-tester --report=json --databases=postgres,mysql,mariadb,cockroachdb,yugabytedb
```

## Command Line Options

### Main Test Command

- `--report` - Report format: `txt`, `json`, or `html` (default: `txt`)
- `--databases` - Comma-separated list of databases to test (default: `postgres,mysql,mariadb,cockroachdb,yugabytedb`; SQL Server is opt-in via `sqlserver` or `mssql`)
- `--scenarios` - Comma-separated list of specific scenarios to run (default: all)
- `--verbose` - Enable verbose output

### List Command

The `list` subcommand helps you discover available test scenarios:

- `--static` - Show only static scenarios (traditional migration tests)
- `--dynamic` - Show only dynamic scenarios (versioned entity evolution tests)
- `--all` - Show all scenarios (default)

Reports are automatically saved to `/app/reports` inside the container and mapped to `./integration/reports` on the host.

## Report Formats

### Text Report
Plain text format suitable for CI/CD pipelines and console output.

### JSON Report
Machine-readable format for integration with other tools and systems.

### HTML Report
Rich, interactive report with:
- Visual progress indicators
- Detailed test results
- Error highlighting
- Responsive design
- Summary statistics

## Database Requirements

### PostgreSQL
- Version: 16+
- Required permissions: CREATE, DROP, SELECT, INSERT, UPDATE, DELETE
- Default schema: `public`

### CockroachDB
- Version: 23+
- Required permissions: CREATE, DROP, SELECT, INSERT, UPDATE, DELETE
- Coverage: opt-in common subset only (`dynamic_cockroachdb_common_subset`)
- Limitations: no `CREATE INDEX CONCURRENTLY`, `DROP INDEX CONCURRENTLY`, XML columns, or advisory locks in the CockroachDB scenario

### YugabyteDB
- Version: 2.25+
- Required permissions: CREATE, DROP, SELECT, INSERT, UPDATE, DELETE
- Coverage: opt-in common subset only (`dynamic_yugabytedb_common_subset`)
- Limitations: no `DROP INDEX CONCURRENTLY` in the YugabyteDB scenario

### MySQL
- Version: 8+
- Required cleanup permissions: global SELECT, DROP, ALTER, ALTER ROUTINE,
  EVENT, LOCK TABLES, PROCESS, and TRIGGER. MySQL 8.0.20 and newer also
  requires SHOW_ROUTINE. Use these credentials only for a disposable dev
  database. Restricted credentials remain suitable for non-cleanup tests.
- Authentication: default MySQL authentication (`caching_sha2_password` on current MySQL images)

### MariaDB
- Version: 10.11+
- Required cleanup permissions: the MySQL cleanup privileges plus global
  SHOW VIEW. Use these credentials only for a disposable dev database.
- Compatible with MySQL driver

### ClickHouse
- Version: 24.11+ for database-realm replay cleanup
- Required permissions: CHECK GRANT, SHOW DATABASES, SHOW TABLES, CREATE DATABASE, DROP DATABASE, and the required DROP privileges for every supported object type in the scratch database
- Cleanup fails before mutation on older servers because complete catalog visibility cannot be proven

### SQL Server
- Version: SQL Server 2022 / Azure SQL compatible subset
- Required permissions: CREATE, DROP, SELECT, INSERT, UPDATE, DELETE in the target database
- Authentication: `github.com/microsoft/go-mssqldb` with `sqlserver://` URLs
- Coverage: opt-in migration fixtures plus `dynamic_sqlserver_identity_schema_bracket_reserved_words`
- Limitations: only scenarios marked `SQLServerCompatible` execute; PostgreSQL-only RLS/functions/roles and unsupported schema-evolution scenarios are skipped before database cleanup

## Test Data

The integration tests use controlled test data:

- **Users table**: Basic user information with email uniqueness
- **Posts table**: Blog posts with foreign key to users
- **Comments table**: Comments with foreign keys to posts and users

This schema provides sufficient complexity to test:
- Primary keys and auto-increment
- Foreign key constraints
- Unique constraints
- Indexes
- Different data types
- Cascading deletes

## Continuous Integration

The integration tests are designed to run in CI/CD environments using Docker Compose:

```yaml
# Example GitHub Actions workflow
- name: Run Integration Tests
  run: |
    docker compose --profile test run --rm ptah-tester --report=json

# Example GitLab CI
test:integration:
  script:
    - docker compose --profile test run --rm ptah-tester --report=json
  artifacts:
    reports:
      junit: integration/reports/*.json

# Example with specific database testing
test:postgres:
  script:
    - docker compose --profile test run --rm ptah-tester --databases=postgres --report=json
```

## Troubleshooting

### Database Connection Issues
- Verify database URLs are correct
- Check that databases are running and accessible
- Ensure proper permissions are granted

### Test Failures
- Check the generated reports for detailed error messages
- Verify test fixtures are properly structured
- Ensure database schemas are clean before running tests

### Performance Issues
- Consider running tests against fewer databases
- Use specific scenario selection for faster feedback
- Check database resource allocation

## Contributing

When adding new test scenarios:

1. Add the scenario function to the appropriate `scenarios_*.go` file
2. Register it in the `GetAllScenarios()` function
3. Create any necessary test fixtures
4. Update this README with scenario documentation
5. Test against all supported databases

## Future Enhancements

- [ ] Performance benchmarking scenarios
- [ ] Large-scale migration testing
- [ ] Cross-database migration compatibility
- [ ] Schema validation scenarios
- [ ] Backup and restore testing
