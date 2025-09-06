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

- 🧱 **Schema Generation (DDL)**
  Builds platform-specific `CREATE TABLE`, `CREATE INDEX`, and other DDL statements.

- 🔐 **Row-Level Security (PostgreSQL)**
  Supports PostgreSQL RLS policies and custom functions for multi-tenant data isolation.

- 🔍 **Database Introspection**
  Reads the current schema directly from Postgres or MySQL for comparison and analysis.

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

- **`parser/`** - SQL DDL token-to-AST parser
  - Converts SQL DDL tokens into Abstract Syntax Tree nodes
  - Supports CREATE TABLE, ALTER TABLE, CREATE INDEX, CREATE TYPE statements
  - Provides comprehensive error handling and position information

- **`lexer/`** - SQL tokenization and lexical analysis
  - Tokenizes SQL input for parser consumption
  - Handles strings, comments, identifiers, operators, and whitespace
  - Provides position tracking for error reporting

- **`renderer/`** - Dialect-specific SQL generation from AST
  - Converts AST nodes to database-specific SQL statements
  - Supports PostgreSQL, MySQL, MariaDB dialects
  - Implements visitor pattern for extensible rendering

- **`platform/`** - Database platform constants and identifiers
  - Defines platform-specific constants used throughout the system
  - Provides standardized platform identification

- **`sqlutil/`** - SQL utility functions
  - SQL statement splitting and comment removal
  - AST-based parsing for proper handling of strings and comments

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
  - Provides dry-run capabilities and transaction safety

- **`planner/`** - Migration planning and SQL generation
  - Converts schema differences into executable SQL statements
  - Dialect-specific planners for PostgreSQL, MySQL, MariaDB
  - Handles dependency ordering and safety checks

- **`schemadiff/`** - Schema comparison and difference analysis
  - Compares generated schemas with live database schemas
  - Identifies tables, columns, indexes, and enum differences
  - Provides detailed change analysis for migration planning

#### `dbschema/` - Database Schema Operations
Handles all database interactions and schema operations:

- **Connection management** for PostgreSQL, MySQL, MariaDB
- **Schema reading and introspection** from live databases
- **Schema writing and migration execution** with transaction support
- **Database cleaning and schema dropping** capabilities
- **Type definitions** for database schema representation

### Command Line Interface

#### `cmd/` - CLI Commands
Provides command-line interface for all Ptah operations:

- **`generate`** - Generate SQL schema from Go entities without touching database
- **`read-db`** - Read and display current database schema
- **`compare`** - Compare Go entities with current database schema
- **`migrate`** - Generate migration SQL for schema differences
- **`migrate-up`** - Apply migrations to bring database up to latest version
- **`migrate-down`** - Roll back migrations to previous versions
- **`migrate-status`** - Show current migration status and history
- **`drop-all`** - Drop ALL tables and enums in database (VERY DANGEROUS!) (supports `--dry-run`)
- **`integration-test`** - Run comprehensive integration tests across database platforms

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

### Field Definition
```go
//migrator:schema:field name="id" type="SERIAL" primary="true" platform.mysql.type="INT AUTO_INCREMENT"
ID int64

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
```

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
- `default_fn` - Default function (e.g., "NOW()")
- `check` - CHECK constraint
- `foreign` - Foreign key reference (table(column))
- `foreign_key_name` - Custom foreign key constraint name
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

```bash
# Clone the repository
git clone https://github.com/stokaro/ptah.git
cd ptah

# Build the CLI tool
go build -o package-migrator ./cmd
```

### Basic Workflow

1. **Define your entities** with schema annotations:

```go
//migrator:schema:table name="users"
type User struct {
    //migrator:schema:field name="id" type="SERIAL" primary="true"
    ID int64

    //migrator:schema:field name="email" type="VARCHAR(255)" not_null="true" unique="true"
    Email string

    //migrator:schema:field name="created_at" type="TIMESTAMP" not_null="true" default_fn="NOW()"
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
./package-migrator generate --root-dir ./models --dialect postgres

# Generate for MySQL
./package-migrator generate --root-dir ./models --dialect mysql
```

3. **Compare and migrate**:

```bash
# Compare current database with Go entities
./package-migrator compare --root-dir ./models --db-url postgres://user:pass@localhost/db

# Generate migration SQL
./package-migrator migrate --root-dir ./models --db-url postgres://user:pass@localhost/db

# Apply migrations to database
./package-migrator migrate-up --db-url postgres://user:pass@localhost/db --migrations-dir ./migrations
```

---

## Command Reference

### Generate Schema
Generate SQL DDL statements from Go entities without touching the database:

```bash
# Generate for all supported dialects
./package-migrator generate --root-dir ./models

# Generate for specific dialect
./package-migrator generate --root-dir ./models --dialect postgres
./package-migrator generate --root-dir ./models --dialect mysql
./package-migrator generate --root-dir ./models --dialect mariadb
```

### Database Operations

#### Read Schema
Read and display the current database schema:

```bash
./package-migrator read-db --db-url postgres://user:pass@localhost:5432/database
```

**Output:** Complete schema information including tables, columns, constraints, indexes, and enums

#### Compare Schemas
Compare your Go entities with the current database schema:

```bash
./package-migrator compare --root-dir ./models --db-url postgres://user:pass@localhost:5432/database
```

**Output:** Detailed differences showing what needs to be added, removed, or modified

#### Generate Migration SQL
Generate SQL migration statements to synchronize schemas:

```bash
./package-migrator migrate --root-dir ./models --db-url postgres://user:pass@localhost:5432/database
```

**Output:** SQL statements to bring the database in sync with Go entities

### Migration Management

Ptah provides a comprehensive migration system with versioning, rollback capabilities, and transaction safety.

#### Apply Migrations
Apply all pending migrations to bring database up to latest version:

```bash
./package-migrator migrate-up --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations

# Dry run to preview what would be applied
./package-migrator migrate-up --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --dry-run
```

**Features:**
- ✅ Applies migrations in correct order based on version numbers
- ✅ Each migration runs in its own transaction
- ✅ Automatic rollback on failure
- ✅ Tracks applied migrations in `schema_migrations` table
- ✅ Supports dry-run mode for preview

#### Roll Back Migrations
Roll back migrations to a specific version:

```bash
./package-migrator migrate-down --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --target 5

# Dry run to preview rollback
./package-migrator migrate-down --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --target 5 --dry-run
```

**Features:**
- ✅ Rolls back to any previous version
- ✅ Executes down migrations in reverse order
- ✅ Transaction safety with automatic rollback on failure
- ✅ Updates migration tracking table

#### Check Migration Status
Show current migration status and pending migrations:

```bash
./package-migrator migrate-status --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations

# JSON output for automation
./package-migrator migrate-status --db-url postgres://user:pass@localhost:5432/database --migrations-dir ./migrations --json
```

**Output includes:**
- Current database version
- Total available migrations
- List of pending migrations
- Migration history and timestamps

#### Migration File Generation
Generate timestamped migration files from schema differences using the migration generator package:

```go
// Example using the migration generator package
package main

import (
    "fmt"
    "log"
    "github.com/stokaro/ptah/migration/generator"
)

func main() {
    opts := generator.GenerateMigrationOptions{
        GoEntitiesDir: "./models",
        DatabaseURL:   "postgres://user:pass@localhost:5432/database",
        MigrationName: "add_user_table",
        OutputDir:     "./migrations",
    }

    files, err := generator.GenerateMigration(opts)
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

files, err := generator.GenerateMigration(opts)
```

**Using Existing Database Connection:**

```go
// Reuse existing connection
conn, err := dbschema.ConnectToDatabase(dbURL)
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

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

### Dangerous Operations

#### Drop All Tables and Enums
Drop ALL tables and enums in the database (VERY DANGEROUS!):

```bash
./package-migrator drop-all --db-url postgres://user:pass@localhost:5432/database

# Dry run to see what would be dropped
./package-migrator drop-all --db-url postgres://user:pass@localhost:5432/database --dry-run
```

**⚠️ Warning:** This command requires double confirmation - you must type 'DELETE EVERYTHING' and then 'YES I AM SURE' to confirm. This will permanently delete ALL data!

### Integration Testing

Run comprehensive integration tests across multiple database platforms:

```bash
# Run all integration tests across all databases
./package-migrator integration-test

# Run tests for specific databases
./package-migrator integration-test --databases postgres,mysql

# Run specific test scenarios
./package-migrator integration-test --scenarios apply_incremental_migrations,rollback_migrations

# Generate detailed HTML report
./package-migrator integration-test --report html

# Verbose output with detailed logging
./package-migrator integration-test --verbose
```

**Features:**
- ✅ Tests across PostgreSQL, MySQL, and MariaDB
- ✅ Comprehensive scenario coverage (basic, concurrency, idempotency, failure recovery)
- ✅ Multiple report formats (TXT, JSON, HTML)
- ✅ Automated database setup and cleanup

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
    "fmt"
    "github.com/stokaro/ptah/core/goschema"
    "github.com/stokaro/ptah/migration/schemadiff"
    "github.com/stokaro/ptah/migration/planner"
    "github.com/stokaro/ptah/dbschema"
)

func main() {
    // Parse Go entities
    generated, err := goschema.ParseDir("./models")
    if err != nil {
        panic(err)
    }

    // Connect to database and read schema
    dbURL := "postgres://user:pass@localhost:5432/database"
    conn, err := dbschema.ConnectToDatabase(dbURL)
    if err != nil {
        panic(err)
    }
    defer conn.Close()

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

Ptah includes a comprehensive integration testing framework that validates migration functionality across PostgreSQL, MySQL, and MariaDB.

#### Run Integration Tests

```bash
# Run all integration tests across all databases
./package-migrator integration-test

# Run tests for specific databases
./package-migrator integration-test --databases postgres,mysql

# Run specific test scenarios
./package-migrator integration-test --scenarios apply_incremental_migrations,rollback_migrations

# Generate detailed HTML report
./package-migrator integration-test --report html

# Verbose output with detailed logging
./package-migrator integration-test --verbose
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

**🔀 Concurrency**
- Launch parallel migrate up processes
- Ensure locking prevents double-apply

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
./package-migrator read-db --db-url postgres://postgres:testpass@localhost:5432/testdb
```

#### MySQL Testing
```bash
# Start MySQL container
docker run --name test-mysql \
  -e MYSQL_ROOT_PASSWORD=testpass \
  -e MYSQL_DATABASE=testdb \
  -p 3306:3306 -d mysql:8.0

# Test with real database
./package-migrator read-db --db-url mysql://root:testpass@tcp(localhost:3306)/testdb
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
- **Transaction Safe**: All operations are wrapped in transactions for consistency

### Supported Databases

- **PostgreSQL** - Full support including enums, constraints (CHECK, UNIQUE, FOREIGN KEY, EXCLUDE), indexes, RLS policies, and extensions
- **MySQL** - Full support with MySQL-specific optimizations
- **MariaDB** - Full support with MariaDB-specific features

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

For PostgreSQL, this creates a proper ENUM type. For MySQL/MariaDB, it uses the ENUM column type.

### Multi-Tenant Row-Level Security (PostgreSQL)

Ptah supports PostgreSQL's Row-Level Security (RLS) for implementing multi-tenant data isolation at the database level:

```go
package main

// Define helper functions for tenant context management
//migrator:schema:function name="set_tenant_context" params="tenant_id_param TEXT" returns="VOID" language="plpgsql" security="DEFINER" body="BEGIN PERFORM set_config('app.current_tenant_id', tenant_id_param, false); END;" comment="Sets the current tenant context for RLS"
//migrator:schema:function name="get_current_tenant_id" returns="TEXT" language="plpgsql" volatility="STABLE" body="BEGIN RETURN current_setting('app.current_tenant_id', true); END;" comment="Gets the current tenant ID from session"

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

**Note:** RLS and custom functions are PostgreSQL-specific features. For other databases, these annotations are ignored during SQL generation.

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
go build -o package-migrator ./cmd
```

---

## License

This project is part of the Inventario system and follows the same licensing terms.

---

## Roadmap

### ✅ Completed Features
- ✅ **Migration versioning and rollback capabilities** - Full migration system with up/down migrations, version tracking, and rollback support
- ✅ **Comprehensive integration testing** - Multi-database testing framework with PostgreSQL, MySQL, and MariaDB support
- ✅ **PostgreSQL extensions support** - Support for PostgreSQL extensions in schema definitions
- ✅ **PostgreSQL EXCLUDE constraints** - Full support for EXCLUDE constraints with USING methods, elements, and WHERE conditions
- ✅ **Migration file generation** - Automatic generation of timestamped migration files from schema differences
- ✅ **Dry-run capabilities** - Preview operations before execution across all commands
- ✅ **Transaction safety** - All operations wrapped in transactions for consistency

### 🚧 In Progress
- [ ] **Enhanced schema validation** - Advanced validation and linting capabilities
- [ ] **Performance optimizations** - Optimizations for large schemas and complex migrations

### 🎯 Planned Features
- [ ] **Additional database dialects** - SQLite, SQL Server support
- [ ] **Web UI for schema visualization** - Interactive schema browser and migration management
- [ ] **Import from existing databases** - Reverse engineering existing schemas to Go entities
- [ ] **Export capabilities** - Export to GraphQL schemas, OpenAPI specs, and other formats
- [ ] **Runtime performance monitoring** - Migration performance tracking and optimization
- [ ] **Schema versioning** - Git-like versioning for schema definitions
- [ ] **Advanced conflict resolution** - Smart handling of schema conflicts and merges

---
