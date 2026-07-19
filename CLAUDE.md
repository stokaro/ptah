# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Ptah is a comprehensive database schema management tool for Go applications. It generates SQL DDL from Go struct annotations,
compares schemas, and manages migrations across PostgreSQL, MySQL, and MariaDB databases.

## Common Commands

### Building and Testing

```bash
# Build the main CLI binary
go build -o bin/ptah ./cmd/ptah

# Build integration test binary
go build -o bin/ptah-integration-test ./cmd/integration-test

# Run unit tests only (fast, no databases)
./test-ptah.sh unit

# Run integration tests (requires Docker)
./test-ptah.sh integration

# Run all tests with reports
./test-ptah.sh

# Using Make targets
make build          # Build all binaries
make test          # Run unit tests
make integration-test  # Run integration tests with Docker
make clean         # Clean build artifacts
```

### CLI Usage

The main binary is built as `ptah`:

```bash
# Generate SQL schema from Go entities
./ptah generate --root-dir ./models --dialect postgres

# Read current database schema
./ptah read-db --db-url postgres://user:pass@localhost/db

# Compare Go entities with database
./ptah compare --root-dir ./models --db-url postgres://user:pass@localhost/db

# Generate migration SQL
./ptah migrate --root-dir ./models --db-url postgres://user:pass@localhost/db

# Apply migrations
./ptah migrate-up --db-url postgres://user:pass@localhost/db --migrations-dir ./migrations

# Rollback migrations
./ptah migrate-down --db-url postgres://user:pass@localhost/db --migrations-dir ./migrations --target 5

# Check migration status
./ptah migrate-status --db-url postgres://user:pass@localhost/db --migrations-dir ./migrations

# Print version metadata
./ptah version

# Drop all tables (DANGEROUS!)
./ptah drop-all --db-url postgres://user:pass@localhost/db --dry-run

# Run integration test suite
./bin/ptah-integration-test --report html --verbose
```

### Docker-based Testing

```bash
# Start databases only for development
make db-start

# Run integration tests against specific database
make integration-test-postgres
make integration-test-mysql
make integration-test-mariadb

# Stop databases
make db-stop

# Clean Docker resources
make docker-clean
```

## Architecture

### Core Components

The system is organized into several key packages:

**`core/`** - Core schema processing components:
- `ast/` - Database-agnostic Abstract Syntax Tree for SQL DDL
- `goschema/` - Go source parsing and entity extraction
- `parser/` - SQL DDL parsing to AST
- `lexer/` - SQL tokenization
- `renderer/` - Dialect-specific SQL generation from AST
- `astbuilder/` - Fluent API for building AST nodes
- `convert/` - Schema format conversions

**`migration/`** - Migration management system:
- `generator/` - Dynamic migration file generation from schema diffs
- `migrator/` - Migration execution engine with rollback support
- `planner/` - Migration planning and SQL generation
- `schemadiff/` - Schema comparison and difference analysis

**`dbschema/`** - Database operations:
- Connection management for PostgreSQL-family targets, MySQL, MariaDB, ClickHouse, and Spanner
- Schema reading/introspection and writing capabilities
- Database cleaning and schema dropping operations

**`cmd/`** - CLI commands:
- Each command is in its own package (generate, migrate, compare, etc.)
- Main binary entry point in `cmd/ptah/main.go`
- Root command assembly in `cmd/root/root.go`

### Key Design Patterns

1. **AST-based SQL Generation** - Uses Abstract Syntax Trees for type-safe, database-agnostic SQL generation
2. **Visitor Pattern** - Enables dialect-specific rendering without modifying core AST
3. **Builder Pattern** - Fluent APIs in `astbuilder/` for constructing complex schemas
4. **Dependency Resolution** - Automatic table creation ordering based on foreign key relationships

### Schema Annotation System

The system reads structured comments from Go structs:

```go
//migrator:schema:table name="products"
type Product struct {
    //migrator:schema:field name="id" type="SERIAL" primary="true"
    ID int64
    
    //migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
    Name string
}

//migrator:schema:index name="idx_products_name" fields="name"
_ int
```

## Testing Architecture

### Unit Tests
- Standard Go tests (`*_test.go`) excluding integration tests
- No external dependencies, run with `go test ./...` or `./test-ptah.sh unit`

### Integration Tests  
- Located in `integration/gonative/` directory
- Use build tag `integration` 
- Require live databases for PostgreSQL-family targets, MySQL, MariaDB, and ClickHouse coverage
- Comprehensive test framework with scenarios covering:
  - Basic migration operations (up/down/status)
  - Idempotency testing
  - Parallel execution smoke
  - Partial failure recovery
  - Schema diff validation

### Test Reporting
- HTML, JSON, and text report generation
- Reports stored in `test-reports/` directory
- Integration with Docker Compose for database setup

## Database Support

**PostgreSQL** - Full support including:
- Extensions, functions, Row-Level Security (RLS)
- Custom types, enums, constraints
- Advanced PostgreSQL features

**MySQL/MariaDB** - Full support with:
- Engine-specific optimizations
- Dialect-specific SQL generation
- Platform-specific overrides in annotations

## Development Guidelines

- All database operations are transaction-safe
- Schema generation is deterministic and dependency-aware  
- CLI commands support `--dry-run` for safety
- Migration files are timestamped and reversible
- Integration tests validate cross-database compatibility

## Important File Locations

- Main CLI entry: `cmd/ptah/main.go`
- Core parsing: `core/goschema/parser.go`
- SQL generation: `core/renderer/renderer.go` 
- Schema comparison: `migration/schemadiff/schemadiff.go`
- Integration tests: `integration/gonative/`
- Example entities: `stubs/`, `examples/`
