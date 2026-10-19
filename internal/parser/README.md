# SQL Parser

This package provides a SQL DDL (Data Definition Language) parser that converts SQL tokens into Abstract Syntax Tree (AST) nodes. The parser is designed to work with the Ptah schema management system and supports multiple SQL dialects.

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

A table element whose first word is none of `CONSTRAINT`, `PRIMARY`, `UNIQUE`,
`FOREIGN`, `CHECK` or `EXCLUDE` is read as a column definition. Those six are
reserved wherever Ptah renders, so no column can be named after them.

`KEY`, `SPATIAL`, `FULLTEXT` and `INDEX` ask the dialect first. Reading them as
an index everywhere refused valid DDL (stokaro/ptah#3089).

Measured on PostgreSQL 17, SQLite 3.51 and MySQL 8, as a bare column name:

| Word | PostgreSQL | SQLite | MySQL |
| --- | --- | --- | --- |
| `key` | accepted | accepted | reserved |
| `spatial` | accepted | accepted | reserved |
| `fulltext` | accepted | accepted | reserved |
| `index` | accepted | reserved | reserved |

MySQL and MariaDB open a table-level index with all four, so they keep the
keyword, and so does the dialect-less best-effort mode, which has no family to
ask. PostgreSQL and SQLite declare no index inside `CREATE TABLE`, so an element
opening with an accepted word there is a column named after it. The parser accepts a type it has never heard of, so
`geometry(Point, 4326)` and a domain declared elsewhere both reach the renderer
verbatim — but it refuses a column whose type carries a parenthesised list whose
every argument is a column of the same table. A type is not an expression and
cannot name a column, so `FULLTEXTT ft_b (bio)` is an index whose keyword is
misspelled rather than a column of type `ft_b(bio)`, and the refusal says so
instead of emitting a table nobody described. ClickHouse is exempt, where
`Nullable(T)` and `Array(T)` make that shape ordinary.

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

### ALTER DEFAULT PRIVILEGES
- `ALTER DEFAULT PRIVILEGES FOR ROLE r IN SCHEMA s GRANT privileges ON TABLES |
  SEQUENCES | FUNCTIONS | TYPES TO grantee [WITH GRANT OPTION]`
- The `REVOKE` form, with an optional `GRANT OPTION FOR` prefix and `FROM grantee`
- `FOR USER`, PostgreSQL's synonym for `FOR ROLE`, naming the same catalog entry

Both scope clauses are required here, where PostgreSQL makes them optional. With
no `FOR ROLE` the grantor is whoever runs the statement, and with no `IN SCHEMA`
the default applies in every schema in the database. The schema model holds
neither, so each is refused rather than read into a declaration that would
render back as a different statement. `ROUTINES` and `SCHEMAS` are refused for
the same reason: no object class in the model spells either.

One role, one schema and one grantee per statement. PostgreSQL accepts a list in
each of the three positions, and reading one here would keep its first name and
drop the rest.

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
  `MERGE`, `SELECT`, `PRAGMA`, `SET`, `BEGIN`, `COMMIT`, and `ROLLBACK` are
  skipped.

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
    
    "ptah.run/internal/parser"
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

The parser reads how a dialect's DDL differs from the dialect's descriptor, in
the `Grammar` section of `core/platform/descriptor`: which words open an index,
which clauses may follow a column, a constraint or a key, which ALTER TABLE
clauses and statements the dialect writes, which routine grammar it uses, and
which spellings the server refuses. A refusal the engine answers in its own
words is held there as the sentence the parser writes. The parser compares no
dialect name, so a dialect added through `platform.Register` is read by what
its descriptor states, with no change to the parser.

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

MySQL and MariaDB dialect mode parses a routine header into
`ast.MySQLRoutineNode` and keeps the body as its text. This package does not
split a MySQL body into statements. The MySQL owner does
(`dialect/mysql/mysqlroutine`), through the runtime's routine body reader
(`core/routineext`).

PostgreSQL-family dialect mode keeps `CREATE FUNCTION` rendering on
`ast.CreateFunctionNode`, but attaches parser-only `PostgresRoutineBody`
metadata for quoted, SQL-standard `RETURN`, and `BEGIN ATOMIC` bodies: the
body text, its dollar-quote delimiter and its language.
In a dialect whose descriptor states the quoted-body routine grammar,
`CREATE PROCEDURE` becomes `ast.PostgresRoutineNode` and a `DO` block becomes
`ast.DoBlockNode`. Every other dialect reads a `DO` block as raw SQL. Both
nodes render through the raw-SQL visitor contract.
This package does not split a PostgreSQL body into statements. The PostgreSQL
owner does (`dialect/postgres/pgroutine`), through the runtime's routine body
reader (`core/routineext`), and a host that analyzes a body asks the runtime.

Parsing a PostgreSQL `DO` body does not execute it. The shared desired-schema
reader hands the body to the reader of the language the dialect's descriptor
names (`internal/doblockext`), which interprets the bounded conditional
role-bootstrap form documented in
[SQL schema](../../docs/site/src/content/docs/schema/postgres-role-bootstrap.md).
It preserves branch selection and refuses unknown procedural effects. Migration
replay remains a separate execution path with disposable-server checks.

SQL Server dialect mode parses `CREATE [OR ALTER] FUNCTION`,
`CREATE [OR ALTER] PROCEDURE`, and `CREATE PROC` through a T-SQL routine layer.
The parser preserves the executable statement in `ast.SQLServerRoutineNode.SQL`,
strips line-scoped client `GO` batch separators from the node, and records
routine metadata for scalar functions, inline table-valued functions,
multi-statement table-valued functions, and procedures. `Returns` is the raw
fragment between `RETURNS` and the body `AS`, so SQL Server routine options
inside that fragment are preserved even when they are not structurally modeled
yet.

The body is kept as its text in `ast.SQLServerRoutineBody`. This package does
not split it into statements: the SQL Server owner does
(`dialect/mssql/mssqlroutine`), through the runtime's routine body reader
(`core/routineext`). Whether a `BEGIN` opens a block and an `END` closes one is
answered by `internal/sqlcompound` for both, so the routine this package
collects and the body the owner splits end at the same `END`.

When a dialect-aware routine boundary is known but the body sub-language is not
structured yet, the parser returns an `ast.OpaqueRoutineNode`: renderers emit its
SQL through the raw-SQL visitor contract, while parser consumers can still
distinguish it from arbitrary raw SQL.

### Owner Syntax

The parser reads the grammar every target shares. A feature owner adds its own
syntax through `ptah.run/core/sqlext`, and `WithSQL` selects the owners a parse
reads with. At each site a pattern may stand, such as an element of a `CREATE
TABLE` column list or an `ALTER TABLE` operation, the parser asks the selected
owners first, matches their opening words against the words at hand, and
records the span a matching pattern claims. Once the document is parsed it
hands every span to its owner in one call and applies what the owner returns.
ClickHouse's data-skipping index and a materialized view's `REFRESH` clause
are read this way. Without `WithSQL` the parse reads the shared grammar alone.

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

PostgreSQL time keywords such as `CURRENT_TIMESTAMP` remain expressions without
added parentheses. Explicit precision, such as `CURRENT_TIMESTAMP(3)`, is kept.
`DEFAULT NULL` is a SQL expression; `DEFAULT 'NULL'` is a string literal.

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
- The options a dialect's descriptor spells as words
  (`descriptor.TableOption.Words`), such as SQLite's `WITHOUT ROWID` and
  `STRICT`. Each is read as the option set to true, in a file of any dialect
  or of none, because a file is also parsed with no dialect. An owner's
  options, the ClickHouse clauses and the options the parser reads itself,
  such as `ENGINE` and `WITH`, are read first.

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

The parser tests cover:
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
- A dialect whose descriptor names a lexer of its own, as YDB's names YQL, is
  not written in the shared grammar, and the parser refuses its documents with
  `ErrUnsupportedFeature`. The engine's owner reads them with its own grammar,
  and `internal/sqlsyntax` serves a syntax tree of them.

## Future Enhancements

Planned improvements:
- Enhanced expression parsing
- Support for more SQL dialects
- Better error recovery
- Performance optimizations
- Extended DDL statement support (DROP TABLE, etc.)
