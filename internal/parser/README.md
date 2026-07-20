# SQL Parser

This package provides a comprehensive SQL DDL (Data Definition Language) parser that converts SQL tokens into Abstract Syntax Tree (AST) nodes. The parser is designed to work with the Ptah schema management system and supports multiple SQL dialects.

## Features

The parser supports the following SQL DDL statements:

### CREATE TABLE
- Column definitions with data types
- Column constraints (PRIMARY KEY, UNIQUE, NOT NULL, AUTO_INCREMENT)
- Default values (literals and function calls)
- Check constraints
- Foreign key references with ON DELETE/UPDATE actions
- Table-level constraints (PRIMARY KEY, UNIQUE, FOREIGN KEY, CHECK)
- Table options (ENGINE, CHARSET, COLLATE, COMMENT)
- Optional `IF NOT EXISTS`
- MySQL-style `CREATE TABLE ... SELECT ...` tails, preserved as raw SELECT SQL
- Unicode identifiers and MySQL identifiers containing `$`

### CREATE SCHEMA / CREATE DATABASE
- Namespace creation with optional `IF NOT EXISTS`

### ALTER TABLE
- ADD COLUMN operations
- DROP COLUMN operations  
- MODIFY/ALTER COLUMN operations
- RENAME COLUMN operations
- RENAME TO table operations
- Schema-qualified table names
- Multiple operations in a single statement

### CREATE INDEX
- Regular indexes
- Unique indexes
- Multi-column indexes

### CREATE VIEW
- `CREATE VIEW ... AS SELECT ...`
- `CREATE OR REPLACE VIEW ... AS SELECT ...`

### CREATE FUNCTION
- PostgreSQL-style `CREATE FUNCTION ... RETURNS ... AS $...$ LANGUAGE ...`
- PostgreSQL-style `CREATE OR REPLACE FUNCTION ... AS '...' LANGUAGE ...`
- Function parameters, return type, language, security, and volatility clauses

### CREATE TYPE (ENUM)
- PostgreSQL-style enum type definitions

### Schema-neutral statements
- DML and session-control statements such as `INSERT`, `UPDATE`, `DELETE`,
  `SELECT`, `PRAGMA`, `SET`, `BEGIN`, `COMMIT`, and `ROLLBACK` are skipped.

These statements are intentionally not represented in the schema AST. They can
appear in migration files alongside DDL, but they do not describe the database
schema that Ptah can diff or render.

## Usage

### Basic Usage

```go
package main

import (
    "fmt"
    "log"
    
    "github.com/stokaro/ptah/internal/parser"
)

func main() {
    sql := `CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        email VARCHAR(255) NOT NULL UNIQUE,
        created_at TIMESTAMP DEFAULT NOW()
    );`
    
    parser := parser.NewParser(sql)
    statements, err := parser.Parse()
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Parsed %d statements\n", len(statements.Statements))
}
```

### Dialect-Aware Parsing

`parser.NewParser(sql)` keeps the compatibility-oriented best-effort behavior
used by existing callers. When the target SQL family is known, pass an explicit
dialect:

```go
p := parser.NewParser(sql, parser.WithDialect(platform.MySQL))
```

Capabilities are separate from dialect identity. Pass them only when the caller
knows the concrete server feature set:

```go
p := parser.NewParser(
    sql,
    parser.WithDialect(platform.Postgres),
    parser.WithCapabilities(capability.Postgres13()),
)
```

Dialect-specific routine bodies are handled behind parser strategies. Generic
mode stays best-effort; dialect mode can use a routine boundary detector that is
specific to the selected SQL family without turning the generic DDL parser into
a stored-procedure sub-language parser.

MySQL and MariaDB dialect mode parses routine headers and top-level routine-body
statement boundaries into `ast.MySQLRoutineNode`. Expressions remain raw SQL
fragments in this slice, but declaration statements now carry typed metadata
for local variables, named conditions, cursors, and handlers. Labels and
control-flow statement classes are still distinguished without parsing scalar
SQL expressions.

PostgreSQL-family dialect mode keeps `CREATE FUNCTION` rendering on
`ast.CreateFunctionNode`, but attaches parser-only `PostgresRoutineBody`
metadata for quoted, SQL-standard `RETURN`, and `BEGIN ATOMIC` bodies.
`CREATE PROCEDURE` and `DO` blocks become PostgreSQL-specific routine nodes in
PostgreSQL-family dialect mode and still render through the raw-SQL visitor
contract.
PL/pgSQL expressions remain raw; the body parser classifies top-level
declarations, block statements, exceptions, returns, dynamic execution, raises,
and control-flow statements.

SQL Server dialect mode parses `CREATE [OR ALTER] FUNCTION`,
`CREATE [OR ALTER] PROCEDURE`, and `CREATE PROC` through a T-SQL routine layer.
The parser preserves the executable statement in `ast.SQLServerRoutineNode.SQL`,
strips line-scoped client `GO` batch separators from the node, and records
routine metadata for scalar functions, inline table-valued functions,
multi-statement table-valued functions, and procedures. `Returns` is the raw
fragment between `RETURNS` and the body `AS`, so SQL Server routine options
inside that fragment are preserved even when they are not structurally modeled
yet. T-SQL scalar expressions remain raw; the body parser classifies
recoverable top-level declarations, assignments, `BEGIN` blocks, `IF`, `WHILE`,
`RETURN`, and `INSERT` / `SELECT` statements.

When a dialect-aware routine boundary is known but the body sub-language is not
structured yet, the parser returns an `ast.OpaqueRoutineNode`: renderers emit its
SQL through the raw-SQL visitor contract, while parser consumers can still
distinguish it from arbitrary raw SQL.

### Parsing Multiple Statements

```go
sql := `
    CREATE TABLE users (id INTEGER PRIMARY KEY);
    CREATE INDEX idx_users_id ON users (id);
    ALTER TABLE users ADD COLUMN name VARCHAR(255);
`

parser := parser.NewParser(sql)
statements, err := parser.Parse()
// statements.Statements will contain 3 AST nodes
```

### Working with AST Nodes

```go
for _, stmt := range statements.Statements {
    switch s := stmt.(type) {
    case *ast.CreateTableNode:
        fmt.Printf("Table: %s\n", s.Name)
        fmt.Printf("Columns: %d\n", len(s.Columns))
        
    case *ast.IndexNode:
        fmt.Printf("Index: %s on %s\n", s.Name, s.Table)
        
    case *ast.AlterTableNode:
        fmt.Printf("Altering table: %s\n", s.Name)
    }
}
```

## Supported SQL Syntax

### Column Types
- Basic types: `INTEGER`, `VARCHAR(255)`, `TEXT`, `TIMESTAMP`
- Parameterized types: `DECIMAL(10,2)`, `CHAR(50)`
- Complex types: `ENUM('value1', 'value2')`

### Column Constraints
- `PRIMARY KEY`
- `NOT NULL` / `NULL`
- `UNIQUE`
- `AUTO_INCREMENT` / `AUTOINCREMENT`
- `DEFAULT value` or `DEFAULT function()`
- `CHECK (expression)`
- `REFERENCES table(column) [ON DELETE action] [ON UPDATE action]`

### Table Constraints
- `PRIMARY KEY (column1, column2)`
- `UNIQUE (column1, column2)`
- `FOREIGN KEY (column) REFERENCES table(column)`
- `CHECK (expression)`
- `CONSTRAINT name PRIMARY KEY (columns)`

### Table Options
- `ENGINE=InnoDB`
- `CHARSET=utf8mb4` / `CHARACTER SET=utf8mb4`
- `COLLATE=utf8mb4_unicode_ci`
- `COMMENT='table description'`
- SQLite `WITHOUT ROWID`
- SQLite `STRICT`

### CREATE TABLE ... SELECT

The parser can represent MySQL-style `CREATE TABLE ... SELECT ...` statements by
storing the SELECT tail on the `CreateTableNode`. The SELECT body is preserved
as SQL text; Ptah does not infer schema columns from SELECT expressions.

Supported forms include:

- `CREATE TABLE IF NOT EXISTS users_copy SELECT * FROM users`
- `CREATE TABLE users_copy ENGINE=heap SELECT * FROM users`
- `CREATE TABLE users_copy (id INT) SELECT id FROM users`

## Architecture

The parser follows a recursive descent parsing approach:

1. **Lexer Integration**: Uses the `ptah/internal/lexer` package for tokenization
2. **AST Generation**: Converts tokens into `ptah/core/ast` nodes
3. **Error Handling**: Provides detailed error messages with position information
4. **Whitespace Handling**: Automatically skips whitespace and comments

### Key Components

- `Parser`: Main parser struct that manages token stream
- `Parse()`: Entry point that returns a `StatementList`
- Statement parsers: `parseCreateTable()`, `parseAlterTable()`, etc.
- Helper parsers: `parseColumnDefinition()`, `parseConstraint()`, etc.

## Error Handling

The parser provides detailed error messages including:
- Expected vs actual token types
- Position information in the input
- Context about what was being parsed

```go
parser := parser.NewParser("CREATE TABLE (id INTEGER);")
_, err := parser.Parse()
// Error: "expected table name: expected identifier, got Operator at position 13"
```

## Testing

The parser includes comprehensive tests covering:
- Basic CREATE TABLE statements
- Complex tables with constraints and options
- ALTER TABLE operations
- CREATE INDEX statements
- CREATE TYPE (ENUM) statements
- Multiple statement parsing
- Error conditions

Run tests with:
```bash
go test -v ./internal/parser
```

## Integration

The parser integrates with other Ptah components:
- **AST Package**: Generates standardized AST nodes
- **Lexer Package**: Consumes SQL tokens
- **Renderer Package**: AST nodes can be rendered back to SQL
- **Migration System**: Parses existing schemas for comparison

## Limitations

Current limitations include:
- Limited to schema DDL statements. DML like `INSERT`, `UPDATE`, and `DELETE`
  is tolerated and skipped when it appears in a migration file, but it is not
  modeled in the AST.
- Basic expression parsing in CHECK constraints
- Simplified handling of complex data types
- Stored procedure and function support is dialect-scoped. MySQL/MariaDB,
  PostgreSQL-family, and SQL Server dialect modes expose routine metadata for
  supported routine forms while preserving expression internals as raw SQL;
  unsupported routine body shapes fall back to typed opaque routines or raw SQL.

## Future Enhancements

Planned improvements:
- Enhanced expression parsing
- Support for more SQL dialects
- Better error recovery
- Performance optimizations
- Extended DDL statement support (DROP TABLE, etc.)
