# Ptah

**Ptah** is a schema management tool for relational databases, inspired by the ancient Egyptian god of creation. In
mythology, Ptah brought the world into existence through thought and speech—shaping order from chaos. This tool follows
a similar philosophy: it turns structured Go code into coherent, executable database schemas, ensuring consistency
between code and data.

The name **Ptah** is also an acronym:

> **P.T.A.H.** — *Parse, Transform, Apply, Harmonize*

- **Parse** – extract schema definitions from annotated Go structs
- **Transform** – generate SQL DDL and schema diffs
- **Apply** – execute up/down migrations with version tracking
- **Harmonize** – synchronize code-defined schema with actual database state

---

## Key Features

`ptah` provides a unified workflow to define, evolve, and apply database schemas based on Go code annotations. Its main
capabilities include:

- 📘 **Go Struct Parsing**
  Extracts tables, columns, indexes, foreign keys, and constraints from structured comments in Go code.

- 🧾 **YAML Schema Input**
  Generates SQL from language-agnostic `.yaml` / `.yml` schema files using the same internal schema model as Go annotations.

- 🧱 **Schema Generation (DDL)**
  Builds platform-specific `CREATE TABLE`, `CREATE INDEX`, and other DDL statements.

- 🔐 **Row-Level Security And Access Control (PostgreSQL)**
  Supports PostgreSQL RLS policies, roles, grants, and custom functions for multi-tenant data isolation.

- 🔍 **Database Introspection**
  Reads the current schema directly from PostgreSQL-family targets, MySQL, MariaDB, SQLite, and ClickHouse for comparison and analysis.

- 🧮 **Schema Diffing**
  Compares code-based schema with the live database schema using AST representations.

- 🪄 **Migration Generation**
  Automatically generates `up` and `down` SQL migrations to bring the database in sync.

- 🚀 **Migration Execution**
  Applies versioned migrations in both directions, tracking state via a migrations table.

- 💥 **Database Cleaning**
  Drops all user-defined schema objects—useful for testing or re-initialization.

---

## Package Structure

Ptah is organized into several key packages that work together to provide comprehensive database schema management:

### Core Packages

#### `core/` - Core Schema Processing Components
The core package contains all fundamental components for parsing, transforming, and representing database schemas:

- **`ast/`** - Abstract Syntax Tree representation for SQL DDL statements
  - Provides database-agnostic AST nodes for CREATE TABLE, ALTER TABLE, CREATE INDEX, etc.
  - Implements visitor pattern for dialect-specific SQL generation
  - Core node types: `CreateTableNode`, `AlterTableNode`, `ColumnNode`, `ConstraintNode`, `IndexNode`, `EnumNode`

- **`astbuilder/`** - Fluent API for building SQL DDL AST nodes
  - Provides convenient builder pattern for constructing complex schemas
  - SchemaBuilder, TableBuilder, ColumnBuilder, IndexBuilder interfaces
  - Integrates seamlessly with the core AST package

- **`goschema/`** - Go package parsing and entity extraction
  - Recursively parses Go source files to discover entity definitions
  - Extracts table directives, field mappings, indexes, enums, and embedded fields
  - Handles dependency analysis and topological sorting for proper table creation order

- **`yamlschema/`** - Language-agnostic YAML schema frontend
  - Parses `.yaml` / `.yml` schema files into the same `goschema.Database` IR used by Go annotations
  - Supports tables, columns, indexes, constraints, enums, extensions, functions, views, materialized views, triggers, RLS, roles, and dialect overrides
  - Uses strict validation for unknown fields, duplicate ordered keys, and invalid constraints

- **`parser/`** - SQL DDL token-to-AST parser
  - Converts SQL DDL tokens into Abstract Syntax Tree nodes
  - Supports CREATE TABLE, ALTER TABLE, CREATE INDEX, CREATE TYPE statements
  - Accepts optional dialect and capability settings for syntax that cannot be parsed correctly in generic best-effort mode
  - Provides comprehensive error handling and position information

- **`lexer/`** - SQL tokenization and lexical analysis
  - Tokenizes SQL input for parser consumption
  - Handles strings, comments, identifiers, operators, and whitespace
  - Provides position tracking for error reporting

- **`renderer/`** - Dialect-specific SQL generation from AST
  - Converts AST nodes to database-specific SQL statements
  - Supports PostgreSQL-family targets, MySQL, MariaDB, SQLite, ClickHouse, and Spanner dialects
  - Implements visitor pattern for extensible rendering

- **`platform/`** - Database platform constants and identifiers
  - Defines platform-specific constants used throughout the system
  - Provides standardized platform identification

- **`sqlutil/`** - SQL utility functions
  - SQL statement splitting and comment removal
  - AST-based parsing for proper handling of strings and comments

- **`sqllint/`** - Standalone SQL lint engine
  - Uses Ptah's parser, AST, and target capability presets for regular SQL files
  - Reports unsupported SQL explicitly so skipped statements cannot look clean
  - Provides reusable rules independent of migration-directory layout

- **`convert/`** - Schema conversion utilities
  - Converts between different schema representations
  - Handles transformations between goschema and database schema formats

#### `migration/` - Migration Management System
Provides comprehensive database migration functionality:

- **`generator/`** - Dynamic migration file generation
  - Generates up/down migration files from schema differences
  - Compares Go entities with current database state
  - Creates timestamped migration files with proper SQL

- **`migrator/`** - Migration execution engine
  - Applies and rolls back database migrations
  - Tracks migration history and versions
  - Provides dry-run capabilities, dirty-state tracking, and dialect-aware transaction handling

- **`planner/`** - Migration planning and SQL generation
  - Converts schema differences into executable SQL statements
  - Dialect-specific planners for PostgreSQL-family targets, MySQL, MariaDB, SQLite, and ClickHouse
  - Handles dependency ordering and safety checks

- **`schemadiff/`** - Schema comparison and difference analysis
  - Compares generated schemas with live database schemas
  - Identifies tables, columns, indexes, and enum differences
  - Provides detailed change analysis for migration planning

#### `dbschema/` - Database Schema Operations
Handles all database interactions and schema operations:

- **Connection management** for PostgreSQL-family targets, MySQL, MariaDB, SQLite, ClickHouse, and Spanner
- **Schema reading and introspection** from live databases (including SQLite `sqlite_schema` / `PRAGMA` metadata and ClickHouse `system.tables` / `system.columns` / `system.data_skipping_indices`)
- **Schema writing and migration execution** with dialect-specific transaction semantics (PostgreSQL-family and SQLite DDL use normal transactions; MySQL/MariaDB DDL implicitly commits; ClickHouse transaction methods are no-ops)
- **Database cleaning and schema dropping** capabilities
- **Type definitions** for database schema representation

### Command Line Interface

#### `cmd/` - CLI Commands
Provides command-line interface for all Ptah operations:

- **`generate`** - Generate SQL schema from Go entities without touching database
- **`read-db`** - Read and display current database schema
- **`compare`** - Compare Go entities with current database schema
- **`drift`** - Check live database drift with CI-friendly exit codes
- **`lint`** - Lint migration files for production-unsafe patterns (rule-coded, CI-friendly)
- **`sql lint`** - Lint standalone SQL files or stdin with parser-backed DDL rules
- **`migrate`** - Generate migration SQL for schema differences
- **`migrate-up`** - Apply migrations to bring database up to latest version
- **`migrate-down`** - Roll back migrations to previous versions
- **`migrate-status`** - Show current migration status and history
- **`migrate-hash`** - Write/update the `ptah.sum` migration-directory integrity file
- **`migrate-validate`** - Verify a migrations directory against its committed `ptah.sum`
- **`seed`** - Apply environment-scoped SQL seed files with replay tracking
- **`drop-all`** - Drop ALL tables and enums in database (VERY DANGEROUS!) (supports `--dry-run`)
- **`ptah-integration-test`** - Run comprehensive integration tests across database platforms

### Supporting Components

#### `examples/` - Usage Examples and Demos
- **`ast_demo/`** - Demonstrates AST-based SQL generation
- **`migrator_parser/`** - Shows parsing and generation workflow

#### `integration/` - Comprehensive Integration Testing Framework
- **`framework.go`** - Core test framework with TestRunner and DatabaseHelper
- **`reporter.go`** - Report generation in multiple formats (TXT, JSON, HTML)
- **`scenarios.go`** - Basic test scenarios implementation
- **`scenarios_advanced.go`** - Advanced test scenarios (concurrency, idempotency)
- **`scenarios_misc.go`** - Miscellaneous test scenarios (timestamps, permissions)
- **`fixtures/`** - Test data including migrations and entity definitions

#### `stubs/` - Example Entity Definitions
Contains sample Go structs with schema annotations for testing and demonstration:
- `product.go`, `category.go` - Real-world entity examples
- Various test files showing different annotation patterns and features

#### `docs/` - Documentation and Design
- **`system_design.md`** - Comprehensive system architecture documentation
- **`diagrams/`** - System architecture diagrams and visual documentation

---

## Go Struct Annotations

Ptah uses structured comments to define database schema information directly in Go structs. Here's the annotation format:

### Table Definition
```go
//migrator:schema:table name="products" platform.mysql.engine="InnoDB" platform.mysql.comment="Product catalog"
type Product struct {
    // fields...
}
```

PostgreSQL-family schemas can be set per table:

```go
//migrator:schema:table name="users" schema="auth"
type User struct {
    // fields...
}
```

Migration generation emits `CREATE SCHEMA IF NOT EXISTS auth` before creating schema-qualified tables and renders references such as `auth.users`.

Schema objects can also be declared explicitly when you need schema-level metadata such as PostgreSQL comments:

```go
//migrator:schema:schema name="auth" comment="Authentication objects"
type AuthSchema struct{}
```

### Field Definition
```go
//migrator:schema:field name="id" type="SERIAL" primary="true" platform.mysql.type="INT AUTO_INCREMENT"
ID int64

//migrator:schema:field name="identity_id" type="INTEGER" not_null="true" identity_generation="BY_DEFAULT" identity_start="10" identity_increment="5"
IdentityID int64

//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
Name string

//migrator:schema:field name="price" type="DECIMAL(10,2)" not_null="true" check="price > 0"
Price float64

//migrator:schema:field name="status" type="ENUM" enum="active,inactive,discontinued" not_null="true" default="active"
Status string

//migrator:schema:field name="category_id" type="INT" not_null="true" foreign="categories(id)" foreign_key_name="fk_product_category"
CategoryID int64
```

### Index Definition
```go
//migrator:schema:index name="idx_products_category" fields="category_id"
_ int

// ClickHouse data-skipping index: opt-in type + granularity
//migrator:schema:index name="idx_events_payload" fields="payload" type="bloom_filter(0.01)" granularity="64"
_ int
```

`type` and `granularity` are honoured only by the ClickHouse renderer (which falls back to `minmax` / `GRANULARITY 8192` when unset). Other dialects ignore them.

### ClickHouse Engine Configuration (ClickHouse only)

ClickHouse tables must declare a storage engine, sorting key, and (optionally) a partition expression. These travel through the existing `platform.<dialect>.*` override syntax:

```go
//migrator:schema:table name="events" platform.clickhouse.engine="MergeTree" platform.clickhouse.order_by="id, created_at" platform.clickhouse.partition_by="toYYYYMM(created_at)" platform.clickhouse.settings="index_granularity=8192"
type Event struct {
    //migrator:schema:field name="id" type="BIGINT" not_null="true"
    ID int64

    //migrator:schema:field name="created_at" type="TIMESTAMP" not_null="true"
    CreatedAt time.Time

    //migrator:schema:field name="payload" type="TEXT"
    Payload string
}
```

Recognized keys: `engine`, `order_by`, `partition_by`, `primary_key`, `sample_by`, `settings`, `ttl`, `comment`. MergeTree-family engines require `order_by`; the renderer rejects a Nullable column that appears in the sorting key.

### PostgreSQL Extensions (PostgreSQL only)
```go
//migrator:schema:extension name="pg_trgm" if_not_exists="true" comment="Trigram similarity for text search"
//migrator:schema:extension name="btree_gin" if_not_exists="true" comment="GIN indexes for btree operations"
//migrator:schema:extension name="postgis" version="3.0" comment="Geographic data support"
type DatabaseExtensions struct{}
```

### PostgreSQL Functions (PostgreSQL only)
```go
//migrator:schema:function name="set_tenant_context" params="tenant_id_param TEXT" returns="VOID" language="plpgsql" security="DEFINER" body="BEGIN PERFORM set_config('app.current_tenant_id', tenant_id_param, false); END;" comment="Sets the current tenant context for RLS"
//migrator:schema:function name="get_current_tenant_id" returns="TEXT" language="plpgsql" volatility="STABLE" body="BEGIN RETURN current_setting('app.current_tenant_id', true); END;" comment="Gets the current tenant ID from session"
type User struct {
    // fields...
}
```

### Views, Materialized Views, And Triggers
```go
//migrator:schema:view name="active_users" body="SELECT id, email FROM users WHERE deleted_at IS NULL" with_check="false"
//migrator:schema:matview name="user_stats" body="SELECT id, COUNT(*) FROM users GROUP BY id" refresh_strategy="manual"
//migrator:schema:trigger name="set_updated_at" table="users" timing="BEFORE" event="UPDATE" for="ROW" body="NEW.updated_at = NOW(); RETURN NEW;"
//migrator:schema:table name="users"
type User struct {
    // fields...
}
```

Views are supported on PostgreSQL, MySQL, and MariaDB. Materialized views are PostgreSQL-only; `refresh_strategy` is authoring metadata for future refresh workflows and is not drift-compared because PostgreSQL does not persist that policy in the catalog. Trigger bodies are dialect-specific: PostgreSQL trigger annotations provide the function body that returns `NEW`/`OLD`, while MySQL/MariaDB trigger bodies are emitted inline.

### Row-Level Security (PostgreSQL only)
```go
// Enable RLS on a table
//migrator:schema:rls:enable table="users" comment="Enable RLS for multi-tenant isolation"

// Create RLS policy
//migrator:schema:rls:policy name="user_tenant_isolation" table="users" for="ALL" to="inventario_app" using="tenant_id = get_current_tenant_id()" comment="Ensures users can only access their tenant's data"

// Policy with WITH CHECK clause for INSERT/UPDATE
//migrator:schema:rls:policy name="product_tenant_isolation" table="products" for="ALL" to="inventario_app" using="tenant_id = get_current_tenant_id()" with_check="tenant_id = get_current_tenant_id()" comment="Ensures products are isolated by tenant"

//migrator:schema:table name="users"
type User struct {
    //migrator:schema:field name="tenant_id" type="TEXT" not_null="true"
    TenantID string
    // other fields...
}
```

### Supported Attributes

#### Field Attributes
- `name` - Database column/table name
- `type` - SQL data type
- `primary` - Primary key constraint
- `not_null` - NOT NULL constraint
- `unique` - UNIQUE constraint
- `default` - Default value
- `default_expr` - Default expression or function call (e.g., `"NOW()"`, `"CURRENT_TIMESTAMP"`, `"gen_random_uuid()"`)
- `check` - CHECK constraint
- `foreign` - Foreign key reference (table(column))
- `foreign_key_name` - Custom foreign key constraint name
- `on_delete` - Foreign key ON DELETE action ("CASCADE", "SET NULL", "RESTRICT", "NO ACTION")
- `on_update` - Foreign key ON UPDATE action ("CASCADE", "SET NULL", "RESTRICT", "NO ACTION")
- `enum` - Enum values (comma-separated)
- `platform.{dialect}.{attribute}` - Platform-specific overrides

#### Extension Attributes (PostgreSQL only)
- `name` - Extension name (e.g., "pg_trgm", "postgis")
- `version` - Specific version to install (optional)
- `if_not_exists` - Skip if extension already exists ("true" or "false")
- `comment` - Extension description

#### Function Attributes (PostgreSQL only)
- `name` - Function name
- `params` - Function parameters (e.g., "tenant_id_param TEXT, user_id INTEGER")
- `returns` - Return type (e.g., "VOID", "TEXT", "INTEGER")
- `language` - Function language (e.g., "plpgsql", "sql")
- `security` - Security context ("DEFINER" or "INVOKER")
- `volatility` - Function volatility ("STABLE", "IMMUTABLE", "VOLATILE")
- `body` - Function implementation code
- `comment` - Function description

#### RLS Policy Attributes (PostgreSQL only)
- `name` - Policy name
- `table` - Target table name
- `for` - Operations policy applies to ("ALL", "SELECT", "INSERT", "UPDATE", "DELETE")
- `to` - Target database roles (e.g., "app_user", "PUBLIC")
- `using` - USING clause expression for row filtering
- `with_check` - WITH CHECK clause expression for INSERT/UPDATE validation
- `comment` - Policy description

#### Role Attributes (PostgreSQL only)
- `name` - Role name
- `login` - Whether the role can login
- `password` - Encrypted password hash
- `superuser` - Whether the role has superuser privileges
- `createdb` or `create_db` - Whether the role can create databases
- `createrole` or `create_role` - Whether the role can create roles
- `inherit` - Whether the role inherits privileges
- `replication` - Whether the role can initiate replication
- `comment` - Role description

#### Grant Attributes (PostgreSQL only)
- `role` - Role receiving the privilege
- `privilege` or `privileges` - One privilege or a comma-separated list such as `SELECT,INSERT,UPDATE,DELETE`
- `on_table` - Target table for table privileges
- `on_schema` - Target schema for schema privileges such as `USAGE`
- `with_option` - Whether to emit `WITH GRANT OPTION`
- `comment` - Grant description

#### Constraint Attributes
- `name` - Constraint name
- `table` - Target table name
- `type` - Constraint type ("CHECK", "UNIQUE", "FOREIGN KEY", "EXCLUDE")
- `columns` - Column names for UNIQUE and FOREIGN KEY constraints (comma-separated)
- `expression` - CHECK constraint expression
- `foreign_table` - Referenced table for FOREIGN KEY constraints
- `foreign_column` - Referenced column for FOREIGN KEY constraints
- `on_delete` - Foreign key ON DELETE action ("CASCADE", "SET NULL", "RESTRICT", "NO ACTION")
- `on_update` - Foreign key ON UPDATE action ("CASCADE", "SET NULL", "RESTRICT", "NO ACTION")
- `using` - Index method for EXCLUDE constraints (e.g., "gist", "btree")
- `elements` - EXCLUDE constraint elements (e.g., "room_id WITH =, during WITH &&")
- `condition` - WHERE condition for EXCLUDE constraints

---

## Quick Start

### Installation

Install the latest released CLI with Go:

```bash
go install github.com/stokaro/ptah/cmd/ptah@latest
ptah version
```

On macOS or Linux with Homebrew:

```bash
brew install stokaro/ptah/ptah
ptah version
```

Use the published container image:

```bash
docker run --rm ghcr.io/stokaro/ptah:latest version
```

Build from source:

```bash
# Clone the repository
git clone https://github.com/stokaro/ptah.git
cd ptah

# Build the CLI tool
go build -o ptah ./cmd/ptah
```

Check build metadata with either command:

```bash
./ptah version
./ptah --version
```

### Environment Variables

Every CLI flag registered through Ptah's shared flag helpers can also be set with
a `PTAH_` environment variable. Hyphens become underscores, so `--db-url` maps
to `PTAH_DB_URL`, `--migrations-dir` maps to `PTAH_MIGRATIONS_DIR`, and
`--dry-run` maps to `PTAH_DRY_RUN`. Explicit CLI flags take precedence over
environment variables.

### Basic Workflow

1. **Define your entities** with schema annotations:

```go
//migrator:schema:table name="users"
type User struct {
    //migrator:schema:field name="id" type="SERIAL" primary="true"
    ID int64

    //migrator:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
    Email string

    //migrator:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_expr="NOW()"
    CreatedAt time.Time
}

// Example with EXCLUDE constraint (PostgreSQL only)
//migrator:schema:table name="bookings"
//migrator:schema:constraint name="no_overlapping_bookings" type="EXCLUDE" table="bookings" using="gist" elements="room_id WITH =, during WITH &&"
type Booking struct {
    //migrator:schema:field name="id" type="SERIAL" primary="true"
    ID int64

    //migrator:schema:field name="room_id" type="INTEGER" not_null="true"
    RoomID int

    //migrator:schema:field name="during" type="TSRANGE" not_null="true"
    During string // PostgreSQL range type
}
```

2. **Generate SQL schema**:

```bash
# Generate for PostgreSQL
./ptah generate --root-dir ./models --dialect postgres

# Generate for MySQL
./ptah generate --root-dir ./models --dialect mysql

# Generate for SQLite
./ptah generate --root-dir ./models --dialect sqlite
```

You can also generate from a language-agnostic YAML schema file or an Atlas HCL
schema file:

```bash
./ptah generate --schema-file schema.yaml --dialect postgres
./ptah generate --schema-file schema.hcl --dialect postgres
```

See [YAML Schema Input](docs/yaml_schema.md) for the supported file format,
validation rules, and examples. See [Atlas HCL Schema Input](docs/atlas_hcl_schema.md)
for the supported Atlas HCL subset and current limitations. See
[Go Annotations vs. Atlas HCL](docs/go_annotations_vs_atlas_hcl.md) for
exporting Go annotations to Atlas HCL and optionally cleaning up source
annotations after a successful export. See [SQLite Support](docs/sqlite.md)
for SQLite URL forms, generated DDL, and ALTER TABLE limitations.

3. **Compare and migrate**:

```bash
# Compare current database with Go entities
./ptah compare --root-dir ./models --db-url postgres://user:pass@localhost/db

# Generate migration SQL
./ptah migrate --root-dir ./models --db-url postgres://user:pass@localhost/db

# Create paired empty migration files for manual SQL
./ptah migrate new add_user_preferences --migrations-dir ./migrations

# Generate migration SQL while introspecting specific PostgreSQL schemas
./ptah migrate --root-dir ./models --db-url postgres://user:pass@localhost/db --schemas auth,billing,public

# Apply migrations to database
./ptah migrate-up --db-url postgres://user:pass@localhost/db --migrations-dir ./migrations
```

---

## Command Reference

### CLI Exit Codes

Ptah uses one exit-code contract across native commands:

| Code | Meaning |
| --- | --- |
| `0` | Success. |
| `1` | Expected negative result: drift, lint findings, migration integrity drift, pending migrations with `--exit-code`, or a non-empty schema diff with `--exit-code`. |
| `2` | Command or usage error: bad flags, unknown command, invalid input, connection failure, parse failure, unsupported dialect, unwritable output, or recovered internal panic. |
| `3+` | Reserved. |

Per-command details are documented in [CLI Exit Codes](docs/exit_codes.md).

### Generate Schema
Generate SQL DDL statements from Go entities without touching the database:

```bash
# Generate for all supported dialects
./ptah generate --root-dir ./models

# Generate for specific dialect
./ptah generate --root-dir ./models --dialect postgres
./ptah generate --root-dir ./models --dialect mysql
./ptah generate --root-dir ./models --dialect mariadb
./ptah generate --root-dir ./models --dialect sqlite
./ptah generate --root-dir ./models --dialect clickhouse

# Generate from a YAML schema file instead of Go annotations
./ptah generate --schema-file schema.yaml --dialect postgres

# Generate from an Atlas HCL schema file instead of Go annotations
./ptah generate --schema-file schema.hcl --dialect postgres
```

### Export Schema

Export Go annotations to Atlas schema HCL:

```bash
./ptah schema export --from go --to atlas-hcl --root-dir ./models --out schema.hcl

# Remove Ptah schema annotations after a successful export
./ptah schema export --root-dir ./models --out schema.hcl --cleanup-go-annotations

# Preview cleanup without modifying Go files
./ptah schema export --root-dir ./models --out schema.hcl --cleanup-go-annotations --cleanup-diff
```

See [Go Annotations vs. Atlas HCL](docs/go_annotations_vs_atlas_hcl.md) for
cleanup semantics, diagnostics, and the currently supported export shape.

### Database Operations

#### Read Schema
Read and display the current database schema:

```bash
./ptah read-db --db-url postgres://user:pass@localhost:5432/database
./ptah read-db --db-url sqlite:///tmp/app.db

# Restrict PostgreSQL introspection to specific schemas
./ptah read-db --db-url postgres://user:pass@localhost:5432/database --schemas auth,billing,public
```

**Output:** Complete schema information including tables, columns, constraints, indexes, and enums

#### Compare Schemas
Compare your Go entities with the current database schema:

```bash
./ptah compare --root-dir ./models --db-url postgres://user:pass@localhost:5432/database
./ptah compare --root-dir ./models --db-url sqlite:///tmp/app.db
./ptah compare --root-dir ./models --db-url postgres://user:pass@localhost:5432/database --schemas auth,billing,public

# Return 1 when the diff is non-empty, 0 when it is empty
./ptah compare --root-dir ./models --db-url postgres://user:pass@localhost:5432/database --exit-code
```

**Output:** Detailed differences showing what needs to be added, removed, or modified

#### Check Schema Drift
Check whether the live database still matches the schema declared by Go entities:

```bash
./ptah drift --root-dir ./models --db-url postgres://user:pass@localhost:5432/database
./ptah drift --root-dir ./models --db-url sqlite:///tmp/app.db
./ptah drift --root-dir ./models --db-url postgres://user:pass@localhost:5432/database --schemas auth,billing,public
```

Use `--format text|json|github-actions` to choose output, and `--ignore tables=audit_log,sessions` to suppress intentionally unmanaged tables. By default any drift returns `1`; use `--severity destructive` to return `1` only for destructive drift such as dropped tables, dropped columns, removed constraints, disabled RLS, or removed database objects.

Nightly GitHub Actions example with Slack notification on drift:

```yaml
name: Nightly schema drift

on:
  schedule:
    - cron: "0 3 * * *"
  workflow_dispatch:

jobs:
  drift:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
      - name: Build ptah
        run: go build -o ptah ./cmd/ptah
      - name: Check schema drift
        id: drift
        continue-on-error: true
        run: |
          ./ptah drift \
            --root-dir ./models \
            --db-url "${{ secrets.STAGING_DATABASE_URL }}" \
            --format github-actions
      - name: Alert Slack on drift
        if: steps.drift.outcome == 'failure'
        env:
          SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}
        run: |
          curl -X POST -H 'Content-type: application/json' \
            --data '{"text":"Ptah schema drift detected on staging. Check the nightly drift workflow."}' \
            "$SLACK_WEBHOOK_URL"
      - name: Fail workflow on drift
        if: steps.drift.outcome == 'failure'
        run: exit 1
```

#### Lint Migration Files
Inspect a migrations directory for production-unsafe patterns, sqlcheck-style:

```bash
./ptah lint --dir ./migrations --dialect postgres
```

Findings carry stable rule codes grouped into families:

- `DS` - data safety: `DS101` dropped table, `DS102` dropped column, `DS103` lossy column type change
- `MF` - migration form: `MF101` missing down file, `MF102` empty migration, `MF103` non-conventional file name
- `BC` - backwards compatibility: `BC101` rename breaking deployed code
- `PG` - PostgreSQL: `PG101` `CREATE INDEX` without `CONCURRENTLY`, `PG102` `ALTER TYPE ... ADD VALUE` in a transaction
- `MY` - MySQL/MariaDB: `MY101` lock-heavy `ALTER TABLE` forms

`--dialect` both gates the dialect-specific rule families and selects the dialect's SQL syntax for scanning (MySQL `#` comments, `/*!...*/` executable comments and backslash string escapes; PostgreSQL dollar quotes and nested block comments). With no dialect set, every rule runs under a hybrid scanner.

`--fail-on` controls whether findings become exit code `1`: `error` by default, `any`, or `none`. `--format text|json|github-actions|sarif` selects the output; the GitHub format annotates PR files inline, and SARIF 2.1.0 can be uploaded to GitHub code scanning. Disable rules per code or family with `--disable DS101 --disable MY`, or persistently via `.ptah-lint.yaml` in the migrations directory:

```yaml
dialect: postgres
disabled-rules:
  - MF103
  - MY
rules:
  DS103:
    severity: warning
  DS102:
    severity: error
    exclude:
      - legacy/**
```

Inline suppressions apply to the next statement only. Ptah accepts both its
native directive and the Atlas-compatible alias:

```sql
-- ptah:nolint DS102
ALTER TABLE users DROP COLUMN legacy_note;

-- atlas:nolint DS103
ALTER TABLE users ALTER COLUMN email TYPE VARCHAR(512);
```

Go integrations can provide custom analyzers without reimplementing Ptah's
dialect-aware scanner by passing `lint.Options{ExtraRules: []lint.Rule{...}}`
or by preparing files first with `lint.PrepareFS`.

#### Lint Standalone SQL
Lint ordinary SQL files outside a migration directory:

```bash
./ptah sql lint --dialect postgres schema.sql
cat schema.sql | ./ptah sql lint --dialect cockroachdb --version "CockroachDB CCL v23.1.0" --stdin
./ptah sql lint --dialect sqlite --format json schema.sql
```

`ptah sql lint` is parser-backed and migration-layout independent. It accepts
file paths or `--stdin`, supports `--format text|json`, and returns exit code
`1` when an error-severity finding is present. Disable rules per code or family
with `--disable DDL001 --disable CAP`. The first built-in rules cover general
DDL quality, such as `DDL001` for `CREATE TABLE` without a primary key, and
capability-aware validation, such as `CAP001` for
`CREATE INDEX CONCURRENTLY` on targets whose preset disables
`create_index_concurrently`.

Unsupported statements are reported as `SQL002` findings instead of being
silently treated as clean. That includes non-DDL statements such as `SELECT`
and DDL forms that Ptah's parser does not yet model.

#### Generate Migration SQL
Generate SQL migration statements to synchronize schemas:

```bash
./ptah migrate --root-dir ./models --db-url postgres://user:pass@localhost:5432/database
./ptah migrate --root-dir ./models --db-url postgres://user:pass@localhost:5432/database --schemas auth,billing,public
```

Use `--report json` when CI needs the destructive-safety verdict as structured
data instead of text:

```bash
./ptah migrate --root-dir ./models --db-url postgres://user:pass@localhost:5432/database --report json
./ptah migrate generate --root-dir ./models --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --report json
```

**Output:** SQL statements to bring the database in sync with Go entities

#### Create Empty Migration Files
Create paired `.up.sql` and `.down.sql` files for manual SQL authoring:

```bash
./ptah migrate new add_user_preferences --migrations-dir ./migrations
./ptah migrate new --name hotfix_existing_data --migrations-dir ./migrations
./ptah atlas migrate new add_user_preferences --migrations-dir ./migrations
```

The command creates timestamped files using Ptah's paired migration naming
convention and writes comment headers for the UP and DOWN directions.

#### Apply Seed Data
Apply environment-scoped SQL seeds from a `seeds/` directory:

```bash
./ptah seed --db-url postgres://user:pass@localhost:5432/database --env test
```

Seed files use `NNN_description.env.sql` names. Files matching `--env` and
`.all.sql` files are applied in version order:

```text
seeds/
  010_countries.all.sql
  020_demo_users.dev.sql
  020_test_users.test.sql
```

Successful files are recorded in `schema_seeds`, so re-running `seed --env test`
is a no-op unless `--force` is set. `--idempotent` uses a per-file savepoint to
treat duplicate-key conflicts as already-applied data; it is rejected on
ClickHouse because ClickHouse does not provide the transactions/savepoints this
mode depends on. Production-like environments (`prod`, `production` by default)
require `--allow-prod`; use `--protected-env name` to configure additional
protected environment names. Use `--protected-table name` to refuse seeding when
the target database already contains production-marker tables unless
`--allow-prod` is explicit.

### Migration Management

Ptah provides a comprehensive migration system with versioning, rollback capabilities, and transaction safety.

Ptah migration directories use `--dir-format=auto` by default. Auto mode prefers
Ptah's paired files (`NNNNNNNNNN_description.up.sql` and
`NNNNNNNNNN_description.down.sql`) when they are present, and otherwise accepts
Atlas-style timestamp files such as `20220318104614_team_A.sql` or
`20240112070806.sql`, plus numeric migration names produced by Atlas importers
such as `1_initial.sql`, `2.sql`, `1_initial.up.sql`, `1_initial.down.sql`, and
`1.my.sql`. Use
`--dir-format=ptah` or `--dir-format=atlas` on `migrate-up`, `migrate-down`,
`migrate-status`, `migrate-hash`, and `migrate-validate` when a directory should
be interpreted explicitly. Ordinary Atlas and imported single SQL files are
forward migrations. Imported `.down.sql` files are paired with their matching
`.up.sql` version for rollback. Atlas txtar files can also carry an embedded
`down.sql` section that Ptah executes on rollback.

```sql
-- atlas:txtar

-- migration.sql --
INSERT INTO users (id, name) VALUES (1, 'Alice');

-- down.sql --
DELETE FROM users WHERE id = 1;
```

For Atlas txtar migrations, Ptah executes only the `migration.sql` section for
`migrate-up` and only the `down.sql` section for `migrate-down`. Other embedded
txtar files, such as `schema.sql`, are ignored by the migrator; ordinary SQL
comments that look like `-- keep this comment --` remain comments, not txtar
section boundaries. Ptah's txtar support is intentionally limited to Atlas SQL
migration containers and is not a general-purpose txtar parser.

Atlas-format SQL template migrations are rendered with Go `text/template`
before execution and linting. Root versioned files such as `1.sql` and `2.sql`
are executable migrations; shared template files in subdirectories can define
helpers such as `{{ define "shared/users" }}` and are not executed as standalone
migrations. The template data object exposes `.Env`; CLI commands set it with
`--atlas-env`, and programmatic callers can pass `WithAtlasTemplateData`.

Ptah reads project defaults from strict `ptah.yaml` named environments and can
also translate a limited Atlas project config subset from `atlas.hcl`.
Supported settings include database URL, dev/shadow URL, schema allow-list,
migration directory/revision-table settings, timeouts, execution order, lint
defaults, and online-DDL routing. Project config precedence is explicit CLI
flags, then `PTAH_*` environment variables, then `atlas.hcl`, then `ptah.yaml`,
then built-in defaults. Use `--env <name>` when multiple env blocks exist; a
single env is selected automatically. See [Ptah Project Config](docs/project_config.md)
and [Atlas Project Config](docs/atlas_project_config.md).

`--dir-format` controls only migration-file discovery. To continue a database
that already uses Atlas's runtime history table, pass `--revision-format atlas`
to `migrate-up`, `migrate-down`, `migrate-status`, `migrate-repair`, or
`migrate-baseline`. Atlas revision mode uses `atlas_schema_revisions` by
default, stores string migration versions, reads the Atlas `applied`/`total` and
`error` state fields, and writes the Atlas `hash` value from `atlas.sum` when it
is available. `--migrations-schema` and `--migrations-table` still override the
metadata table location when needed.

If an Atlas migration has no embedded `down.sql`, `migrate-down` fails with a
typed error explaining that Atlas dynamic down-plan synthesis is not implemented
yet. This is different from transaction rollback: transaction rollback undoes a
failed in-progress migration automatically, while `migrate-down` reverts an
already-recorded migration using an explicit Ptah `.down.sql` file or an Atlas
txtar `down.sql` section.

`-- +ptah` directives inside `migration.sql` and `down.sql` are parsed per
section for timeout and validation purposes. The current `no_transaction` model
is still migration-level: if either direction opts out of transactions, Ptah
treats the migration as non-transactional.

#### Apply Migrations
Apply all pending migrations to bring database up to latest version:

```bash
./ptah migrate-up --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations

# Production rollout defaults: fail fast on hot-table locks and runaway statements
./ptah migrate-up --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --lock-timeout 3s --statement-timeout 30s --migration-lock-timeout 30s

# Dry run to preview what would be applied
./ptah migrate-up --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --dry-run

# Apply a migration that was merged below the current high-water mark
./ptah migrate-up --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --exec-order non-linear

# Store migration state in a dedicated schema/table
./ptah migrate-up --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --migrations-schema infra --migrations-table ptah_migrations

# Apply an Atlas-style versioned migration directory
./ptah migrate-up --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --dir-format atlas

# Continue an Atlas-managed database using atlas_schema_revisions
./ptah migrate-up --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --dir-format atlas --revision-format atlas

# Apply Atlas SQL template migrations with .Env set to dev
./ptah migrate-up --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --dir-format atlas --atlas-env dev
```

Migration files can override the CLI defaults with top-of-file directives:

```sql
-- +ptah lock_timeout=3s
-- +ptah statement_timeout=30s
```

PostgreSQL uses `SET LOCAL lock_timeout` and `SET LOCAL statement_timeout` inside the migration transaction. MySQL and MariaDB use `SET SESSION innodb_lock_wait_timeout`; statement timeouts use `max_execution_time` on MySQL and `max_statement_time` on MariaDB. Generated migrations that contain `ALTER TABLE` include the recommended `3s` lock timeout and `30s` statement timeout directives for supported dialects.

`migrate-up`, `migrate-down`, and programmatic `MigrateTo` acquire a
session-level migration advisory lock around the planning and apply window for
PostgreSQL, MySQL, and MariaDB. PostgreSQL uses `pg_advisory_lock`; MySQL and
MariaDB use `GET_LOCK('ptah_migrate', timeout)`. By default Ptah waits until
the lock is available; MariaDB does not accept MySQL's negative infinite
timeout, so Ptah uses a long server-side wait there unless an explicit timeout
is configured. Set `--migration-lock-timeout` to make concurrent runners fail
with a typed migration lock timeout error that callers can detect with
`migrator.IsMigrationLockTimeout`.

Most migrations run inside the normal per-migration transaction. If the database
rejects transactional execution, use an explicit file directive:

```sql
-- +ptah no_transaction
ALTER TYPE status ADD VALUE 'archived';
ALTER TABLE users ALTER COLUMN status SET DEFAULT 'archived';
```

Use this only for narrow database requirements such as PostgreSQL enum value
additions that must be used later in the same migration. Ptah rejects migration
timeouts on `no_transaction` migrations because those statements run as raw
autocommit SQL instead of through the transaction-bound writer.

Generated PostgreSQL migrations also use this path for indexes on populated
existing tables. If live introspection reports an existing table with an
estimated row count greater than zero, `migrate generate` emits
`CREATE INDEX CONCURRENTLY` for new indexes on that table and writes the file
with `-- +ptah no_transaction`. When a change set also contains ordinary
transactional DDL, Ptah splits the output into separate migration versions:
transactional changes first, then concurrent indexes. PostgreSQL-family targets
whose capability preset disables concurrent indexes, including YugabyteDB and
CockroachDB, keep regular `CREATE INDEX` output.

Ptah detects out-of-order migrations from the applied version set. By default,
`migrate-up` uses `--exec-order=linear` and fails if a pending migration version is
below the current high-water mark, which catches ordinary branch merge races instead
of silently reporting "up to date". Use `--exec-order=non-linear` to apply those
pending lower versions, or `--exec-order=linear-skip` to leave them unapplied with
a warning.

Ptah records migration state before executing each migration and marks the row
`applied` only after the migration finishes. A failed migration leaves a dirty
`schema_migrations` revision with statement progress, error text, execution
time, and a SHA-256 checksum of the up SQL. `migrate-up`, `migrate-down`, and
`migrate-to` refuse to continue while a dirty revision exists, so operators can
inspect the database instead of accidentally running later migrations on a
half-applied schema.

After fixing the database state manually, use `migrate-repair` to mark the
dirty revision resolved:

```bash
./ptah migrate-repair --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --version 12

# For non-transactional engines, resume remaining up statements before marking applied.
./ptah migrate-repair --db-url mysql://user:pass@tcp(localhost:3306)/database --migrations-dir ./migrations --version 12 --resume-from 2
```

Already-applied migrations are also checked against the current up SQL checksum.
If a migration file is edited after it was applied, Ptah aborts before planning
new work and reports a checksum mismatch for that version.

#### Baseline an Existing Database

Use `migrate-baseline` when adopting Ptah on a database whose schema already
exists. The command initializes the migration metadata table and records every
migration at or below the baseline version as `applied` without executing those
migration SQL bodies. By default the baseline version is the highest migration
in the directory.

```bash
# Strong verification: replay migrations on a disposable shadow database, then
# compare the shadow schema to the existing target schema before writing metadata.
./ptah migrate-baseline --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --shadow-db postgres://user:pass@localhost:5432/ptah_shadow

# Preview exactly which rows would be written and which metadata table is used.
./ptah migrate-baseline --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --dry-run

# Baseline through a specific version and use a custom metadata table.
./ptah migrate-baseline --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --version 20260718120000 --migrations-schema infra --migrations-table ptah_migrations
```

Without `--shadow-db`, Ptah falls back to a weaker entity drift check: it parses
Go entities from `--root-dir`, introspects the target database, and refuses to
baseline if they differ. This does not prove that the migration files reproduce
the target schema; use a disposable shadow database for production brownfield
adoption. `migrate-baseline` refuses to write into a non-empty metadata table
unless `--force` is set. With `--force`, verification failures are treated as
explicit operator overrides, and existing metadata rows at or below the baseline
version can be updated. Ptah still refuses to rewrite history if the metadata
table already contains revisions above the requested baseline version.

**Features:**
- ✅ Applies migrations in correct order based on version numbers
- ✅ Detects out-of-order pending migrations below the current version
- ✅ PostgreSQL-family migrations run in transactions unless explicitly marked `no_transaction`
- ✅ Tracks dirty/failed migration state and refuses to continue until repaired
- ✅ Verifies applied migration checksums before running new work
- ✅ Baselines existing databases by stamping migration metadata without executing DDL
- ✅ Executes Ptah paired down files and Atlas txtar `down.sql` sections
- ✅ Tracks applied migrations in `schema_migrations` table, or a custom `--migrations-schema`/`--migrations-table`
- ✅ Supports dry-run mode for preview
- ✅ Supports per-migration lock and statement timeout directives
- ✅ Online DDL for large MySQL/MariaDB tables via gh-ost / pt-online-schema-change

Transaction semantics are engine-specific. PostgreSQL-family DDL can roll back
when a migration fails inside a transaction. MySQL and MariaDB implicitly commit
most DDL, so a failed multi-statement migration can leave earlier DDL applied;
Ptah records dirty migration state so operators can inspect and repair before
continuing. ClickHouse transaction hooks are no-ops in Ptah because
multi-statement transactions are experimental and require explicit session
setup outside Ptah's current protection model.

**Online DDL (MySQL/MariaDB):** route lock-heavy `ALTER TABLE` statements through
[gh-ost](https://github.com/github/gh-ost) or
[pt-online-schema-change](https://docs.percona.com/percona-toolkit/pt-online-schema-change.html) —
either per migration with a `-- +ptah online_ddl_tool=ghost` directive, or automatically for
tables above a row threshold via `ptah.yaml` (`--config`):

```yaml
online_ddl:
  tool: ghost            # or pt-osc
  threshold_rows: 1000000
  args: ["--allow-on-master"]
```

If the tool is not on PATH, ptah warns and falls back to a plain `ALTER TABLE`.
See [docs/online-ddl.md](docs/online-ddl.md) for prerequisites (binlog ROW format,
privileges, topology flags) and invocation details.

#### Roll Back Migrations
Roll back migrations to a specific version:

```bash
./ptah migrate-down --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --target 5

# Use the same production safety defaults for rollback DDL
./ptah migrate-down --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --target 5 --lock-timeout 3s --statement-timeout 30s --migration-lock-timeout 30s

# Dry run to preview rollback
./ptah migrate-down --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --target 5 --dry-run

# Roll back using the same custom migration state table
./ptah migrate-down --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --target 5 --migrations-schema infra --migrations-table ptah_migrations
```

**Features:**
- ✅ Rolls back to any previous version
- ✅ Executes down migrations in reverse order
- ✅ Transactional rollback on PostgreSQL-family engines; dirty-state tracking on engines where DDL cannot be rolled back
- ✅ Updates migration tracking table

#### Check Migration Status
Show current migration status and pending migrations:

```bash
./ptah migrate-status --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations

# JSON output for automation
./ptah migrate-status --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --json

# Return 1 when pending migrations are available, 0 when there are none
./ptah migrate-status --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --exit-code

# Check status from a custom migration state table
./ptah migrate-status --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --migrations-schema infra --migrations-table ptah_migrations

# Check an Atlas-style versioned migration directory
./ptah migrate-status --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --dir-format atlas

# Check an Atlas-managed revisions table
./ptah migrate-status --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --dir-format atlas --revision-format atlas

# Check Atlas SQL template migrations with .Env set to dev
./ptah migrate-status --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --dir-format atlas --atlas-env dev

# Read db URL, migration directory, and Atlas revision-table settings from atlas.hcl
./ptah migrate-status --env local --json
```

**Output includes:**
- Current database version
- Applied migration versions
- Total available migrations
- List of pending migrations
- Out-of-order pending migrations
- Migration history and timestamps

#### Atlas-Compatible CLI Namespace And Exit Codes

Ptah keeps the existing kebab-case commands as native commands, and reserves an
Atlas-compatible command tree under `ptah atlas <command> ...` for scripts that
expect Atlas OSS command paths. The native command tree remains separate while
it is redesigned independently.

| Atlas-compatible command | Native command |
| --- | --- |
| `ptah atlas migrate apply` | `ptah migrate-up` |
| `ptah atlas migrate down` | `ptah migrate-down` |
| `ptah atlas migrate status` | `ptah migrate-status` |
| `ptah atlas migrate hash` | `ptah migrate-hash` |
| `ptah atlas migrate validate` | `ptah migrate-validate` |
| `ptah atlas migrate lint` | `ptah lint` |
| `ptah atlas schema inspect` | `ptah read-db` |
| `ptah atlas schema diff` | `ptah compare` |

Atlas-compatible aliases accept the Atlas OSS flag names on the compatibility
surface and translate implemented flags to the native Ptah flags before
execution. For example, `ptah atlas migrate apply --url ... --dir ...` maps to
`ptah migrate-up --db-url ... --migrations-dir ...`, and
`ptah atlas schema inspect --url ...` maps to `ptah read-db --db-url ...`.
Flags that are part of the Atlas OSS CLI but do not have Ptah behavior yet are
advertised and rejected explicitly instead of being silently ignored.

The implemented Atlas-compatible commands delegate to native commands after
flag translation, so their exit codes follow the native command contract
documented in [CLI Exit Codes](docs/exit_codes.md). Unsupported Atlas-compatible
flags are rejected explicitly and exit `2`.

#### Migration Directory Integrity (`ptah.sum` / `atlas.sum`)

Once a migration is committed and applied somewhere, its content must never change.
`ptah.sum` records the SHA-256 (`h1:`, base64) of every migration file plus a
directory-level hash, so an out-of-band edit to an already-applied migration is
caught in CI instead of silently breaking reproducibility.

```bash
# Write/update ptah.sum after adding or intentionally editing a migration
./ptah migrate-hash --dir ./migrations

# Verify the directory matches its committed ptah.sum (CI gate)
./ptah migrate-validate --dir ./migrations

# Hash or validate an Atlas-style migration directory
./ptah migrate-hash --dir ./migrations --dir-format atlas
./ptah migrate-validate --dir ./migrations --dir-format atlas
```

Ptah-format integrity writes `ptah.sum` and hashes the Ptah migration files that
would be executed. Atlas-format integrity writes `atlas.sum` and uses Atlas's
byte-compatible checksum algorithm: top-level `*.sql` files sorted
lexicographically, chained per-file hashes, directory hashes without separators,
and `-- atlas:sum ignore` support. Auto validation reads `atlas.sum` when it is
the only integrity file present, reads `ptah.sum` otherwise, and errors if both
files exist so CI does not silently choose the wrong format.

`migrate-validate` exits `1` on integrity drift (a file added, removed, or
edited out of band, with a diff on stderr). `migrate-up --verify-sum` runs the
same check before applying and aborts on drift.

```yaml
# CI (GitHub Actions)
- name: Verify migration integrity
  run: ./ptah migrate-validate --dir ./migrations
```

The line-oriented file layout and `h1:` scheme are shared by both formats, but
the hash algorithm differs. Recompute the integrity file with the selected
format instead of renaming or hand-editing an existing sum file.

#### Migration File Generation
Generate timestamped migration files from schema differences using the migration generator package:

```go
// Example using the migration generator package
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/stokaro/ptah/migration/generator"
)

func main() {
    opts := generator.GenerateMigrationOptions{
        GoEntitiesDir:      "./models",
        DatabaseURL:        "postgres://user:pass@localhost:5432/database",
        MigrationName:      "add_user_table",
        OutputDir:          "./migrations",
        Schemas:            []string{"auth", "billing", "public"},
        ShadowDatabaseURL:  "postgres://user:pass@localhost:5432/shadow_database",
    }

    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    files, err := generator.GenerateMigration(ctx, opts)
    if err != nil {
        log.Fatal(err)
    }

    // Check if any migration was generated (nil means no changes detected)
    if files == nil {
        fmt.Println("No schema changes detected - no migration needed")
        return
    }

    fmt.Printf("Generated: %s and %s\n", files.UpFile, files.DownFile)
}
```

**Generated files:**
- `YYYYMMDDHHMMSS_add_user_table.up.sql` - Forward migration
- `YYYYMMDDHHMMSS_add_user_table.down.sql` - Rollback migration

**Advanced Usage with Embedded Filesystem:**

```go
//go:embed entities
var entitiesFS embed.FS

opts := generator.GenerateMigrationOptions{
    GoEntitiesDir: ".",
    GoEntitiesFS:  entitiesFS,  // Use embedded filesystem
    DatabaseURL:   "postgres://user:pass@localhost:5432/database",
    MigrationName: "add_user_table",
    OutputDir:     "./migrations",
}

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

files, err := generator.GenerateMigration(ctx, opts)
```

**CLI generation with shadow verification:**

```bash
ptah migrate generate \
  --root-dir ./models \
  --db-url postgres://user:pass@localhost:5432/database \
  --migrations-dir ./migrations \
  --name add_user_table \
  --shadow-db postgres://user:pass@localhost:5432/shadow_database
```

`--shadow-db` is destructive for the shadow database: Ptah drops all objects in
that database, replays existing migrations, applies the candidate migration,
re-introspects the schema, and only writes files when the replayed schema
matches the Go source. A mismatch aborts before writing files with a diagnostic
such as `shadow check failed: missing column users.email`. Library callers can
inspect structured shadow mismatches via `generator.ShadowVerificationError`.

You can make this the default for `migrate generate` by configuring the same
disposable database in `ptah.yaml`; an explicit `--shadow-db` value still wins:

```yaml
migrate:
  generate:
    shadow_db: postgres://user:pass@localhost:5432/shadow_database
```

**Using Existing Database Connection:**

```go
// Reuse existing connection. Supply a context so a stuck host cannot block
// the initial Ping; defer dbschema.CloseAndWarn so close errors are surfaced.
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
if err != nil {
    log.Fatal(err)
}
defer dbschema.CloseAndWarn(conn)

opts := generator.GenerateMigrationOptions{
    GoEntitiesDir: "./models",
    DBConn:        conn,  // Reuse connection instead of creating new one
    MigrationName: "add_user_table",
    OutputDir:     "./migrations",
}
```

**Features:**
- ✅ Automatic timestamp-based versioning
- ✅ Generates both up and down migrations
- ✅ Compares Go entities with live database
- ✅ Handles table, column, index, and constraint changes
- ✅ Database-specific SQL generation
- ✅ Embedded filesystem support for Go modules
- ✅ Connection reuse for better performance
- ✅ No-op detection (returns nil when no changes needed)
- ✅ Optional shadow database replay before files are written

### Dangerous Operations

#### Drop All Tables and Enums
Drop ALL tables and enums in the database (VERY DANGEROUS!):

```bash
./ptah drop-all --db-url postgres://user:pass@localhost:5432/database

# Dry run to see what would be dropped
./ptah drop-all --db-url postgres://user:pass@localhost:5432/database --dry-run
```

**⚠️ Warning:** This command requires double confirmation - you must type 'DELETE EVERYTHING' and then 'YES I AM SURE' to confirm. This will permanently delete ALL data!

### Integration Testing

Run comprehensive integration tests across multiple database platforms:

```bash
# Run all integration tests across all databases
./bin/ptah-integration-test

# Run tests for specific databases
./bin/ptah-integration-test --databases postgres,mysql

# Run specific test scenarios
./bin/ptah-integration-test --scenarios apply_incremental_migrations,rollback_migrations

# Generate detailed HTML report
./bin/ptah-integration-test --report html

# Verbose output with detailed logging
./bin/ptah-integration-test --verbose
```

**Features:**
- ✅ Tests across PostgreSQL-family targets, MySQL, MariaDB, and ClickHouse
- ✅ Comprehensive scenario coverage (basic, parallel execution smoke, idempotency, failure recovery)
- ✅ Multiple report formats (TXT, JSON, HTML)
- ✅ Automated database setup and cleanup
- ✅ ClickHouse scenarios are opt-in per scenario (`ClickHouseCompatible`); incompatible scenarios skip cleanly against a ClickHouse connection
- ✅ PostgreSQL-family distributed SQL scenarios are opt-in per scenario (`PostgresDistributedCompatible`), with live common-subset coverage for CockroachDB and YugabyteDB

---

## Programming Examples

### Using the AST API

```go
package main

import (
    "fmt"
    "github.com/stokaro/ptah/core/ast"
    "github.com/stokaro/ptah/core/renderer"
)

func main() {
    // Create a table using the AST API
    table := ast.NewCreateTable("users").
        AddColumn(
            ast.NewColumn("id", "SERIAL").
                SetPrimary().
                SetAutoIncrement(),
        ).
        AddColumn(
            ast.NewColumn("email", "VARCHAR(255)").
                SetNotNull().
                SetUnique(),
        ).
        AddColumn(
            ast.NewColumn("created_at", "TIMESTAMP").
                SetDefaultFunction("CURRENT_TIMESTAMP"),
        ).
        AddConstraint(ast.NewUniqueConstraint("uk_users_email", "email"))

    // Render for PostgreSQL
    pgSQL, err := renderer.RenderSQL("postgresql", table)
    if err != nil {
        panic(err)
    }
    fmt.Println("PostgreSQL:")
    fmt.Println(pgSQL)

    // Render for MySQL
    mysqlSQL, err := renderer.RenderSQL("mysql", table)
    if err != nil {
        panic(err)
    }
    fmt.Println("MySQL:")
    fmt.Println(mysqlSQL)
}
```

### Parsing Go Packages

```go
package main

import (
    "fmt"
    "github.com/stokaro/ptah/core/goschema"
    "github.com/stokaro/ptah/core/renderer"
)

func main() {
    // Parse Go entities from a directory
    result, err := goschema.ParseDir("./models")
    if err != nil {
        panic(err)
    }

    // Generate ordered CREATE TABLE statements
    statements := renderer.GetOrderedCreateStatements(result, "postgresql")

    for _, stmt := range statements {
        fmt.Println(stmt)
    }
}
```

### Schema Comparison

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/stokaro/ptah/core/goschema"
    "github.com/stokaro/ptah/dbschema"
    "github.com/stokaro/ptah/migration/planner"
    "github.com/stokaro/ptah/migration/schemadiff"
)

func main() {
    // Parse Go entities
    generated, err := goschema.ParseDir("./models")
    if err != nil {
        panic(err)
    }

    // Connect to database and read schema. Supply a context so the initial
    // Ping cannot block indefinitely on a stuck host.
    dbURL := "postgres://user:pass@localhost:5432/database"
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
    if err != nil {
        panic(err)
    }
    defer dbschema.CloseAndWarn(conn)

    database, err := conn.Reader().ReadSchema()
    if err != nil {
        panic(err)
    }

    // Compare schemas
    diff := schemadiff.Compare(generated, database)

    // Generate migration SQL
    migrationSQL := planner.GenerateSchemaDiffSQLStatements(diff, generated, "postgres")
    for _, stmt := range migrationSQL {
        fmt.Println(stmt)
    }
}
```

---

## Testing

Ptah includes a comprehensive testing framework with both unit tests and integration tests across multiple database platforms.

### Running Unit Tests

```bash
# Run all unit tests (no database required)
go test ./...

# Run tests with verbose output
go test -v ./...

# Run specific package tests
go test -v ./core/...
go test -v ./migration/...
```

### Integration Testing Framework

Ptah includes a comprehensive integration testing framework that validates migration functionality across PostgreSQL-family targets, MySQL, MariaDB, and ClickHouse. CockroachDB and YugabyteDB have live common-subset scenarios in CI; Spanner uses capability, planning, and rendering coverage only.

#### Run Integration Tests

```bash
# Run all integration tests across all databases
./bin/ptah-integration-test

# Run tests for specific databases
./bin/ptah-integration-test --databases postgres,mysql

# Run specific test scenarios
./bin/ptah-integration-test --scenarios apply_incremental_migrations,rollback_migrations

# Generate detailed HTML report
./bin/ptah-integration-test --report html

# Verbose output with detailed logging
./bin/ptah-integration-test --verbose
```

#### Test Coverage

The integration test suite covers:

**🧱 Basic Functionality**
- Apply incremental migrations
- Roll back migrations
- Upgrade to specific version
- Check current version
- Generate desired schema
- Read actual DB schema
- Dry-run support
- Operation planning
- Schema diff validation
- Failure diagnostics

**🔁 Idempotency**
- Re-apply already applied migrations
- Run migrate up when database is already up-to-date

**🔀 Parallel Execution Smoke**
- Launch two migrate up processes in parallel
- Verify at least one runner succeeds and the final migration state is consistent
- Ptah does not yet provide a migration-level lock; enforce a single production runner externally until #124 lands

**🧪 Partial Failure Recovery**
- Handle multi-step migrations with intentional failures
- Validate recovery and rollback capabilities

**⏱ Timestamp Verification**
- Check that `applied_at` timestamps are stored correctly

**📂 Manual Patch Detection**
- Detect manual schema changes via schema diff

**🔒 Permission Restrictions**
- Test behavior with limited database privileges

**🧹 Cleanup Support**
- Drop all tables and re-run from empty state

### Database Testing

For integration tests, you can use Docker to set up test databases:

#### PostgreSQL Testing
```bash
# Start PostgreSQL container
docker run --name test-postgres \
  -e POSTGRES_PASSWORD=testpass \
  -e POSTGRES_DB=testdb \
  -p 5432:5432 -d postgres:15

# Run tests
go test -v ./executor/... -tags=integration

# Test with real database
./ptah read-db --db-url postgres://postgres:testpass@localhost:5432/testdb
```

#### MySQL Testing
```bash
# Start MySQL container
docker run --name test-mysql \
  -e MYSQL_ROOT_PASSWORD=testpass \
  -e MYSQL_DATABASE=testdb \
  -p 3306:3306 -d mysql:8.0

# Test with real database
./ptah read-db --db-url mysql://root:testpass@tcp(localhost:3306)/testdb
```

---

## Architecture

### Data Flow

1. **Parse** - Go source files are parsed to extract schema annotations
2. **Transform** - Annotations are converted to internal schema representations
3. **Generate** - Schema representations are converted to AST nodes
4. **Render** - AST nodes are rendered to dialect-specific SQL
5. **Execute** - SQL is executed against the target database

### Key Design Principles

- **Database Agnostic**: Core logic works with any supported database
- **AST-Based**: Uses Abstract Syntax Trees for type-safe SQL generation
- **Visitor Pattern**: Enables dialect-specific rendering without modifying core AST
- **Dependency Aware**: Automatically handles table creation order based on foreign keys
- **Dialect-Aware Safety**: PostgreSQL-family DDL uses transactions; MySQL/MariaDB implicit commits and ClickHouse no-op transactions are documented and tracked through dirty migration state

### Supported Databases

This matrix describes workflow readiness. It is separate from the lower-level
DDL capability matrix in [docs/capabilities.md](docs/capabilities.md).

| Target | Parse + render | Introspect | Diff | Apply | Transactional apply | Live CI coverage | Production-supported |
| --- | --- | --- | --- | --- | --- | --- | --- |
| PostgreSQL | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| MySQL | Yes | Yes | Yes | Yes | No; DDL implicitly commits | Yes | Yes, with MySQL DDL caveats |
| MariaDB | Yes | Yes | Yes | Yes | No; DDL implicitly commits | Yes | Yes, with MariaDB DDL caveats |
| ClickHouse | Yes, compatible MergeTree subset | Yes | Yes, compatible subset | Yes, compatible subset | No; transaction hooks are no-ops | Yes | Limited to compatible scenarios |
| CockroachDB | PostgreSQL-family common subset | Yes | Yes, common subset | Yes, common subset | Yes for supported transactional DDL | Yes | Limited to common-subset workflows |
| YugabyteDB | PostgreSQL-family common subset | Yes | Yes, common subset | Yes, common subset | Yes for supported transactional DDL | Yes | Limited to common-subset workflows |
| Spanner | Conservative PostgreSQL-interface routing | Partial/offline-oriented | Partial/offline-oriented | Not production-ready | No verified live apply path | No | No |

---

## Advanced Features

### Platform-Specific Overrides

You can specify platform-specific attributes in your annotations:

```go
//migrator:schema:table name="products" platform.mysql.engine="InnoDB" platform.mysql.comment="Product catalog"
type Product struct {
    //migrator:schema:field name="id" type="SERIAL" platform.mysql.type="INT AUTO_INCREMENT" platform.mariadb.type="INT AUTO_INCREMENT"
    ID int64
}
```

### Embedded Fields

Ptah supports embedded fields with different relation modes:

```go
type Address struct {
    Street string
    City   string
}

//migrator:schema:table name="users"
type User struct {
    ID int64

    // Embedded as separate columns
    //migrator:schema:embedded mode="columns"
    Address Address

    // Embedded as JSON
    //migrator:schema:embedded mode="json" name="address_data" type="JSONB"
    Metadata Address
}
```

### Enums

Define enums for type safety:

```go
//migrator:schema:field name="status" type="ENUM" enum="active,inactive,pending" not_null="true" default="active"
Status string
```

For PostgreSQL, this creates a proper ENUM type. For MySQL/MariaDB, it uses the ENUM column type. ClickHouse maps `ENUM` fields to inline `Enum8`/`Enum16` column types based on the declared values.

### Multi-Tenant Row-Level Security (PostgreSQL)

Ptah supports PostgreSQL's Row-Level Security (RLS) for implementing multi-tenant data isolation at the database level:

```go
package main

// Define helper functions for tenant context management
//migrator:schema:function name="set_tenant_context" params="tenant_id_param TEXT" returns="VOID" language="plpgsql" security="DEFINER" body="BEGIN PERFORM set_config('app.current_tenant_id', tenant_id_param, false); END;" comment="Sets the current tenant context for RLS"
//migrator:schema:function name="get_current_tenant_id" returns="TEXT" language="plpgsql" volatility="STABLE" body="BEGIN RETURN current_setting('app.current_tenant_id', true); END;" comment="Gets the current tenant ID from session"

// Define the application role and the privileges it needs.
//migrator:schema:role name="app_role" inherit="true" comment="Application runtime role"
//migrator:schema:grant role="app_role" privilege="USAGE" on_schema="public" comment="Allow app role to resolve public schema objects"
//migrator:schema:grant role="app_role" privilege="SELECT,INSERT,UPDATE,DELETE" on_table="users" comment="Allow app role to use tenant-scoped users"
//migrator:schema:grant role="app_role" privilege="SELECT,INSERT,UPDATE,DELETE" on_table="products" comment="Allow app role to use tenant-scoped products"
type AccessControl struct{}

// Enable RLS and create policies for users table
//migrator:schema:rls:enable table="users" comment="Enable RLS for multi-tenant isolation"
//migrator:schema:rls:policy name="user_tenant_isolation" table="users" for="ALL" to="app_role" using="tenant_id = get_current_tenant_id()" comment="Ensures users can only access their tenant's data"
//migrator:schema:table name="users" comment="User accounts table"
type User struct {
    //migrator:schema:field name="id" type="SERIAL" primary="true"
    ID int64 `json:"id" db:"id"`

    //migrator:schema:field name="tenant_id" type="TEXT" not_null="true"
    TenantID string `json:"tenant_id" db:"tenant_id"`

    //migrator:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
    Email string `json:"email" db:"email"`

    //migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
    Name string `json:"name" db:"name"`
}

// Enable RLS and create policies for products table with INSERT/UPDATE checks
//migrator:schema:rls:enable table="products" comment="Enable RLS for product isolation"
//migrator:schema:rls:policy name="product_tenant_isolation" table="products" for="ALL" to="app_role" using="tenant_id = get_current_tenant_id()" with_check="tenant_id = get_current_tenant_id()" comment="Ensures products are isolated by tenant"
//migrator:schema:table name="products" comment="Product catalog table"
type Product struct {
    //migrator:schema:field name="id" type="SERIAL" primary="true"
    ID int64 `json:"id" db:"id"`

    //migrator:schema:field name="tenant_id" type="TEXT" not_null="true"
    TenantID string `json:"tenant_id" db:"tenant_id"`

    //migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
    Name string `json:"name" db:"name"`

    //migrator:schema:field name="user_id" type="INTEGER" not_null="true" foreign="users(id)"
    UserID int64 `json:"user_id" db:"user_id"`
}
```

This generates the following PostgreSQL SQL:

```sql
-- Create the runtime role and grant object privileges
CREATE ROLE app_role WITH NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOREPLICATION;
GRANT USAGE ON SCHEMA public TO app_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE users TO app_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE products TO app_role;

-- Create helper functions
CREATE OR REPLACE FUNCTION set_tenant_context(tenant_id_param TEXT) RETURNS VOID AS $$
BEGIN PERFORM set_config('app.current_tenant_id', tenant_id_param, false); END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION get_current_tenant_id() RETURNS TEXT AS $$
BEGIN RETURN current_setting('app.current_tenant_id', true); END;
$$ LANGUAGE plpgsql STABLE;

-- Enable RLS on tables
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE products ENABLE ROW LEVEL SECURITY;

-- Create RLS policies
CREATE POLICY user_tenant_isolation ON users FOR ALL TO app_role
    USING (tenant_id = get_current_tenant_id());

CREATE POLICY product_tenant_isolation ON products FOR ALL TO app_role
    USING (tenant_id = get_current_tenant_id())
    WITH CHECK (tenant_id = get_current_tenant_id());
```

**Note:** RLS, roles, grants, and custom functions are PostgreSQL-specific features. For other databases, these annotations are ignored during SQL generation.

### EXCLUDE Constraints (PostgreSQL)

Ptah supports PostgreSQL's EXCLUDE constraints for preventing overlapping or conflicting data:

```go
package main

// Prevent overlapping room bookings
//migrator:schema:table name="bookings"
//migrator:schema:constraint name="no_overlapping_bookings" type="EXCLUDE" table="bookings" using="gist" elements="room_id WITH =, during WITH &&"
type Booking struct {
    //migrator:schema:field name="id" type="SERIAL" primary="true"
    ID int64

    //migrator:schema:field name="room_id" type="INTEGER" not_null="true"
    RoomID int

    //migrator:schema:field name="during" type="TSRANGE" not_null="true"
    During string // PostgreSQL range type
}

// EXCLUDE constraint with WHERE condition
//migrator:schema:table name="user_sessions"
//migrator:schema:constraint name="one_active_session_per_user" type="EXCLUDE" table="user_sessions" using="gist" elements="user_id WITH =" condition="is_active = true"
type UserSession struct {
    //migrator:schema:field name="id" type="SERIAL" primary="true"
    ID int64

    //migrator:schema:field name="user_id" type="INTEGER" not_null="true"
    UserID int

    //migrator:schema:field name="is_active" type="BOOLEAN" not_null="true" default="false"
    IsActive bool
}
```

**Generated SQL:**
```sql
-- For bookings table
ALTER TABLE bookings ADD CONSTRAINT no_overlapping_bookings
    EXCLUDE USING gist (room_id WITH =, during WITH &&);

-- For user_sessions table
ALTER TABLE user_sessions ADD CONSTRAINT one_active_session_per_user
    EXCLUDE USING gist (user_id WITH =) WHERE (is_active = true);
```

**EXCLUDE Constraint Features:**
- **Index Methods**: Supports `gist`, `btree`, and other PostgreSQL index methods
- **Multiple Elements**: Can exclude on multiple columns with different operators
- **WHERE Conditions**: Optional WHERE clause for conditional exclusion
- **Operator Classes**: Supports PostgreSQL operator classes (=, &&, <>, etc.)

**Common Use Cases:**
- **Room Booking Systems**: Prevent overlapping time slots for the same resource
- **Session Management**: Ensure only one active session per user
- **Spatial Data**: Prevent overlapping geographic regions
- **Scheduling**: Avoid conflicting appointments or events

**Note:** EXCLUDE constraints are PostgreSQL-specific. For other databases, these annotations generate warnings during migration planning.

---

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

### Development Setup

```bash
# Clone the repository
git clone https://github.com/stokaro/ptah.git
cd ptah

# Install dependencies
go mod download

# Run tests
go test ./...

# Build the CLI
go build -o ptah ./cmd/ptah
```

---

## License

This project is part of the Inventario system and follows the same licensing terms.

---

## Roadmap

### ✅ Completed Features
- ✅ **Migration versioning and rollback capabilities** - Full migration system with up/down migrations, version tracking, and rollback support
- ✅ **Comprehensive integration testing** - Multi-database testing framework with PostgreSQL-family, MySQL, MariaDB, and ClickHouse coverage
- ✅ **ClickHouse dialect** - MergeTree-family engine annotations, data-skipping indexes, and live introspection via `system.tables`
- ✅ **CockroachDB and YugabyteDB common-subset support** - PostgreSQL-family rendering, planning, and live integration coverage for supported features
- ✅ **Spanner capability routing** - Conservative PostgreSQL-interface planning/rendering presets without live production support
- ✅ **PostgreSQL extensions support** - Support for PostgreSQL extensions in schema definitions
- ✅ **PostgreSQL EXCLUDE constraints** - Full support for EXCLUDE constraints with USING methods, elements, and WHERE conditions
- ✅ **YAML schema frontend** - Language-agnostic schema files rendered through the same internal model as Go annotations
- ✅ **Atlas HCL schema input and Go annotation export** - Parse supported Atlas schema HCL and export Go annotations to Atlas HCL
- ✅ **Migration linting** - Rule-coded production-safety linting with text, JSON, GitHub Actions, and SARIF output
- ✅ **Schema drift checks** - CI-friendly live drift detection with stable exit codes
- ✅ **Migration file generation** - Automatic generation of timestamped migration files from schema differences
- ✅ **Brownfield baseline workflow** - Adopt existing databases by verifying and stamping migration metadata without replaying DDL
- ✅ **Online DDL hooks** - Optional gh-ost / pt-online-schema-change routing for large MySQL/MariaDB alters
- ✅ **Migration directory integrity** - `ptah.sum` / `atlas.sum` hashing and validation
- ✅ **Seed runner** - Environment-scoped SQL seed execution with production guards
- ✅ **Stable CLI exit-code contract** - Documented `0` / `1` / `2` process semantics for scripting
- ✅ **Dry-run capabilities** - Preview operations before execution across all commands
- ✅ **Dialect-aware transaction handling** - PostgreSQL-family transactional DDL with documented MySQL/MariaDB and ClickHouse caveats

### 🚧 In Progress
- [ ] **Performance optimizations** - Optimizations for large schemas and complex migrations

### 🎯 Planned Features
- [ ] **Additional database dialects** - SQL Server support
- [ ] **Web UI for schema visualization** - Interactive schema browser and migration management
- [ ] **Import from existing databases** - Reverse engineering existing schemas to Go entities
- [ ] **Export capabilities** - Export to GraphQL schemas, OpenAPI specs, and other formats
- [ ] **Runtime performance monitoring** - Migration performance tracking and optimization
- [ ] **Schema versioning** - Git-like versioning for schema definitions
- [ ] **Advanced conflict resolution** - Smart handling of schema conflicts and merges

---
