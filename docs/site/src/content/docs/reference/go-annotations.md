---
title: Go annotation reference
description: Every //ptah directive and attribute accepted by Ptah's Go annotation parser.
---

This page lists every `//ptah` comment directive and attribute Ptah's Go
annotation parser accepts. The same metadata is exported as a JSON Schema
document by `ptah schema annotations`, and the committed copy lives at
[`schemas/ptah-annotations.schema.json`](https://github.com/stokaro/ptah/blob/master/schemas/ptah-annotations.schema.json).
For the workflow — modeling, rendering, and generating migrations from
annotated structs — see [Go annotations](../../schema/go-annotations/).

## Syntax

A directive is a single Go comment line: the directive name followed by
space-separated `key="value"` attributes.

```go
//ptah:schema:table name="products"
```

- Attributes marked "bare form allowed" may be written without a value:
  `primary` is equivalent to `primary="true"`.
- An unknown attribute fails parsing with
  `unknown annotation attribute "<name>" on //<directive>`.
- Directives marked "Platform overrides: yes" also accept
  `platform.<dialect>.<attribute>="..."` pairs that override the base
  attribute for one dialect, for example
  `platform.mysql.type="JSON" platform.mariadb.type="LONGTEXT"`.
- The Required column records what the parser rejects. An attribute the
  renderer needs for valid SQL (such as `name` on a table) can still be
  omitted at parse time; the rendered SQL is then invalid. `ptah schema
  render` is the cheapest way to catch that early.

## Placement

Each directive attaches to one of three places in Go source:

- **struct** — the comment lines directly above a `type ... struct`
  declaration. Any struct works, including empty marker structs declared only
  to carry directives, and one struct can carry several directives.
- **field** — the comment line directly above a struct field. Directives that
  allow both struct and field placement (`index`, `constraint`) can use a
  blank `_ int` placeholder field inside the struct.
- **file** — a detached comment line anywhere in a Go file, separated from
  declarations by blank lines (row-level security directives only). The named
  `table` must be declared in the same file; a file-scoped RLS comment naming
  a table from another file is silently ignored.

```go
//ptah:schema:table name="users"
//ptah:schema:constraint name="users_email_check" type="CHECK" check="email <> ''"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="email" type="VARCHAR(255)" not_null="true"
	Email string

	//ptah:schema:index name="idx_users_email" fields="email"
	_ int
}

//ptah:schema:enum name="user_status" values="active,disabled"
type StatusEnumMarker struct{}
```

## Directive index

| Directive | Declares | Placement |
| --- | --- | --- |
| [`ptah:schema:table`](#ptahschematable) | A database table | struct |
| [`ptah:schema:field`](#ptahschemafield) | A table column | field |
| [`ptah:embedded`](#ptahembedded) | Columns or relations from an embedded Go field | field |
| [`ptah:schema:index`](#ptahschemaindex) | An index | struct or field |
| [`ptah:schema:constraint`](#ptahschemaconstraint) | A table constraint | struct or field |
| [`ptah:schema:enum`](#ptahschemaenum) | A reusable enum type | struct |
| [`ptah:schema:domain`](#ptahschemadomain) | A PostgreSQL domain type | struct |
| [`ptah:schema:composite`](#ptahschemacomposite) | A PostgreSQL composite type | struct |
| [`ptah:schema:range`](#ptahschemarange) | A PostgreSQL range type | struct |
| [`ptah:schema:schema`](#ptahschemaschema) | A database schema/namespace | struct |
| [`ptah:schema:extension`](#ptahschemaextension) | A PostgreSQL extension | struct |
| [`ptah:schema:sequence`](#ptahschemasequence) | A standalone PostgreSQL sequence | struct |
| [`ptah:schema:function`](#ptahschemafunction) | A database function | struct |
| [`ptah:schema:trigger`](#ptahschematrigger) | A database trigger | struct |
| [`ptah:schema:view`](#ptahschemaview) | A database view | struct |
| [`ptah:schema:matview`](#ptahschemamatview) | A materialized view | struct |
| [`ptah:schema:role`](#ptahschemarole) | A database role | struct |
| [`ptah:schema:grant`](#ptahschemagrant) | Database grants | struct |
| [`ptah:schema:rls:enable`](#ptahschemarlsenable) | Row-level security enablement | file or struct |
| [`ptah:schema:rls:policy`](#ptahschemarlspolicy) | A row-level security policy | file or struct |
| [`ptah:schema:data`](#ptahschemadata) | Reference/seed row data for a table | struct |

## Tables and columns

### `//ptah:schema:table`

Maps a Go struct to a database table.

| Attribute | Required | Description |
| --- | --- | --- |
| `checks` | No | Comma-separated table-level check expressions. |
| `comment` | No | Table comment. |
| `custom` | No | Raw custom CREATE TABLE SQL. |
| `engine` | No | MySQL/MariaDB table engine shortcut. |
| `name` | No | Table name. |
| `primary_key` | No | Comma-separated primary key columns. |
| `schema` | No | Database schema name. |

Platform overrides: yes.

### `//ptah:schema:field`

Maps a Go struct field to a database column.

| Attribute | Required | Description |
| --- | --- | --- |
| `auto_increment` | No | Marks the column as auto-incrementing. `true`/`false`; bare form allowed. |
| `check` | No | Column CHECK expression. |
| `check_name` | No | Explicit CHECK constraint name. |
| `comment` | No | Column comment. |
| `default` | No | Literal column default. |
| `default_expr` | No | SQL default expression. |
| `enum` | No | Comma-separated enum values. |
| `foreign` | No | Foreign key reference in table(column) form. |
| `foreign_key_name` | No | Explicit foreign key constraint name. |
| `generated` | No | Generated column expression. |
| `generated_kind` | No | Generated column kind, such as STORED or VIRTUAL. |
| `identity_generation` | No | SQL identity generation mode. |
| `identity_increment` | No | SQL identity increment value. |
| `identity_options` | No | Raw SQL identity options. |
| `identity_start` | No | SQL identity start value. |
| `name` | No | Column name. |
| `not_null` | No | Marks the column NOT NULL. `true`/`false`; bare form allowed. |
| `on_delete` | No | Foreign key ON DELETE action. |
| `on_update` | No | Foreign key ON UPDATE action. |
| `primary` | No | Marks the column as part of the primary key. `true`/`false`; bare form allowed. |
| `stored` | No | Shortcut controlling generated column storage. `true`/`false`. |
| `type` | No | Database column type. |
| `unique` | No | Adds a single-column unique constraint. `true`/`false`; bare form allowed. |
| `unique_expr` | No | Unique expression for dialects that support expression indexes. |

Platform overrides: yes.

### `//ptah:embedded`

Controls how an embedded Go field contributes schema objects.

| Attribute | Required | Description |
| --- | --- | --- |
| `comment` | No | Generated column comment. |
| `field` | No | Generated relation field name. |
| `mode` | No | Embedding mode: inline, json, or relation. |
| `name` | No | Column name for json embedding. |
| `nullable` | No | Marks generated embedded columns nullable. `true`/`false`; bare form allowed. |
| `on_delete` | No | Generated foreign key ON DELETE action. |
| `on_update` | No | Generated foreign key ON UPDATE action. |
| `prefix` | No | Column prefix for inline embedded fields. |
| `ref` | No | Relation target in table(column) form. |
| `type` | No | Generated column type for json or relation embedding. |

Platform overrides: yes.

For relation embedding, set `type` to the referenced column's physical type.
When it is omitted, Ptah uses a conservative numeric or string heuristic,
which cannot infer every user-defined or dialect-specific key type.

### `//ptah:schema:index`

Declares an index for a table.

| Attribute | Required | Description |
| --- | --- | --- |
| `columns` | No | Synonym for `fields`. |
| `comment` | No | Index comment. |
| `condition` | No | Partial index condition. |
| `fields` | No | Comma-separated Go field or column names. |
| `granularity` | No | ClickHouse data-skipping index granularity. |
| `name` | No | Index name. |
| `nulls_distinct` | No | Controls NULLS DISTINCT behavior where supported. `true`/`false`. |
| `ops` | No | PostgreSQL operator class. |
| `table` | No | Explicit target table. |
| `type` | No | Index type or method. |
| `unique` | No | Creates a unique index. `true`/`false`; bare form allowed. |
| `where` | No | Atlas-style partial index condition alias. |

### `//ptah:schema:constraint`

Declares a table constraint.

| Attribute | Required | Description |
| --- | --- | --- |
| `check` | No | CHECK expression. |
| `columns` | No | Comma-separated local columns. |
| `comment` | No | Constraint comment. |
| `condition` | No | Constraint WHERE condition. |
| `elements` | No | EXCLUDE constraint elements. |
| `foreign_column` | No | Single referenced column for FOREIGN KEY constraints. |
| `foreign_columns` | No | Comma-separated referenced columns for composite FOREIGN KEY constraints. |
| `foreign_table` | No | Referenced table for FOREIGN KEY constraints. |
| `include` | No | Comma-separated PostgreSQL INCLUDE columns for covering UNIQUE constraints. |
| `name` | No | Constraint name. |
| `nulls_distinct` | No | Controls NULLS DISTINCT behavior where supported. `true`/`false`. |
| `on_delete` | No | Foreign key ON DELETE action. |
| `on_update` | No | Foreign key ON UPDATE action. |
| `table` | No | Explicit target table. |
| `type` | No | Constraint type: CHECK, UNIQUE, PRIMARY KEY, FOREIGN KEY, or EXCLUDE. |
| `using` | No | EXCLUDE index method. |

## Reusable types

### `//ptah:schema:enum`

Declares a reusable enum type.

| Attribute | Required | Description |
| --- | --- | --- |
| `name` | Yes | Enum type name. |
| `values` | Yes | Comma-separated enum values. |

### `//ptah:schema:domain`

Declares a PostgreSQL domain type.

| Attribute | Required | Description |
| --- | --- | --- |
| `check` | No | CHECK constraint expression (uses VALUE). |
| `comment` | No | Domain comment. |
| `default` | No | Literal DEFAULT value. |
| `default_expr` | No | DEFAULT expression. |
| `name` | Yes | Domain name. |
| `not_null` | No | Marks the domain NOT NULL. `true`/`false`. |
| `schema` | No | Target schema/namespace. |
| `type` | Yes | Underlying base data type. |

### `//ptah:schema:composite`

Declares a PostgreSQL composite type.

| Attribute | Required | Description |
| --- | --- | --- |
| `comment` | No | Composite type comment. |
| `fields` | Yes | Comma-separated name:type field list. |
| `name` | Yes | Composite type name. |
| `schema` | No | Target schema/namespace. |

### `//ptah:schema:range`

Declares a PostgreSQL range type.

| Attribute | Required | Description |
| --- | --- | --- |
| `canonical` | No | Canonicalization function. |
| `collation` | No | Collation for the subtype. |
| `comment` | No | Range type comment. |
| `name` | Yes | Range type name. |
| `schema` | No | Target schema/namespace. |
| `subtype` | Yes | Element subtype the range is built over. |
| `subtype_diff` | No | Subtype difference function. |
| `subtype_opclass` | No | Operator class for the subtype. |

## Database objects

### `//ptah:schema:schema`

Declares a database schema or namespace.

| Attribute | Required | Description |
| --- | --- | --- |
| `comment` | No | Schema comment. |
| `name` | Yes | Schema name. |

### `//ptah:schema:extension`

Declares a PostgreSQL extension.

| Attribute | Required | Description |
| --- | --- | --- |
| `comment` | No | Extension comment. |
| `if_not_exists` | No | Adds IF NOT EXISTS where supported. `true`/`false`. |
| `name` | No | Extension name. |
| `version` | No | Extension version. |

### `//ptah:schema:sequence`

Declares a standalone PostgreSQL sequence.

| Attribute | Required | Description |
| --- | --- | --- |
| `as` | No | Underlying integer type, such as bigint. |
| `cache` | No | CACHE size. |
| `comment` | No | Sequence comment. |
| `cycle` | No | Enables CYCLE wrap-around. `true`/`false`. |
| `if_not_exists` | No | Adds IF NOT EXISTS where supported. `true`/`false`. |
| `increment` | No | INCREMENT BY value; must be non-zero. |
| `maxvalue` | No | MAXVALUE bound. |
| `minvalue` | No | MINVALUE bound. |
| `name` | Yes | Sequence name. |
| `owned_by` | No | Owning table.column association (OWNED BY). |
| `schema` | No | Target schema/namespace. |
| `start` | No | START WITH value. |

### `//ptah:schema:function`

Declares a database function.

| Attribute | Required | Description |
| --- | --- | --- |
| `body` | No | Function body SQL. |
| `comment` | No | Function comment. |
| `language` | No | Function language. |
| `name` | No | Function name. |
| `params` | No | Function parameter list. |
| `returns` | No | Return type. |
| `security` | No | Security mode, such as DEFINER. |
| `volatility` | No | Volatility class. |

### `//ptah:schema:trigger`

Declares a database trigger.

| Attribute | Required | Description |
| --- | --- | --- |
| `body` | Yes | Trigger body SQL. |
| `comment` | No | Trigger comment. |
| `event` | Yes | Trigger event, such as INSERT or UPDATE. |
| `for` | No | Trigger granularity; defaults to ROW. |
| `name` | Yes | Trigger name. |
| `table` | Yes | Target table. |
| `timing` | Yes | Trigger timing, such as BEFORE or AFTER. |

### `//ptah:schema:view`

Declares a database view.

| Attribute | Required | Description |
| --- | --- | --- |
| `body` | Yes | View SELECT body. |
| `comment` | No | View comment. |
| `name` | Yes | View name. |
| `with_check` | No | Controls WITH CHECK OPTION where supported. `true`/`false`. |

### `//ptah:schema:matview`

Declares a materialized view.

| Attribute | Required | Description |
| --- | --- | --- |
| `body` | Yes | Materialized view SELECT body. |
| `comment` | No | Materialized view comment. |
| `name` | Yes | Materialized view name. |
| `refresh_strategy` | No | Refresh strategy; defaults to manual. |

## Security

### `//ptah:schema:role`

Declares a database role.

| Attribute | Required | Description |
| --- | --- | --- |
| `comment` | No | Role comment. |
| `create_db` | No | Alias for `createdb`. `true`/`false`. |
| `create_role` | No | Alias for `createrole`. `true`/`false`. |
| `createdb` | No | Allows database creation. `true`/`false`. |
| `createrole` | No | Allows role creation. `true`/`false`. |
| `inherit` | No | Controls role inheritance; defaults to true. `true`/`false`. |
| `login` | No | Creates the role with LOGIN. `true`/`false`. |
| `name` | No | Role name. |
| `password` | No | Role password. |
| `replication` | No | Allows replication. `true`/`false`. |
| `superuser` | No | Creates the role as SUPERUSER. `true`/`false`. |

### `//ptah:schema:grant`

Declares database grants.

| Attribute | Required | Description |
| --- | --- | --- |
| `comment` | No | Grant comment. |
| `grant_option` | No | Alias for `with_option`. `true`/`false`. |
| `on_schema` | No | Target schema. |
| `on_sequence` | No | Target sequence. |
| `on_table` | No | Target table. |
| `privilege` | No | Privilege or comma-separated privileges. |
| `privileges` | No | Alias for `privilege`. |
| `role` | No | Target role. |
| `with_option` | No | Adds WITH GRANT OPTION where supported. `true`/`false`. |

### `//ptah:schema:rls:enable`

Enables row-level security on a table.

| Attribute | Required | Description |
| --- | --- | --- |
| `comment` | No | RLS enablement comment. |
| `table` | No | Target table. |

### `//ptah:schema:rls:policy`

Declares a row-level security policy.

| Attribute | Required | Description |
| --- | --- | --- |
| `comment` | No | Policy comment. |
| `for` | No | Policy command, such as ALL or SELECT. |
| `name` | No | Policy name. |
| `table` | No | Target table. |
| `to` | No | Comma-separated roles. |
| `using` | No | USING expression. |
| `with_check` | No | WITH CHECK expression. |

## Reference data

### `//ptah:schema:data`

Declares external reference/seed row data for a table.

| Attribute | Required | Description |
| --- | --- | --- |
| `file` | Yes | Path to the YAML row-data file, relative to the Go source file. |
| `key` | Yes | Comma-separated key column(s) forming each row's identity. |
| `schema` | No | Database schema the table belongs to. |
| `table` | Yes | Target table the rows belong to. |

## Editor support

The repository ships `ptah-ls`, a language server for `//ptah` annotations
in Go source. It speaks the Language Server Protocol over stdio and provides
hover documentation, attribute completion, and diagnostics backed by the same
directive metadata as this page. Build it from the repository root:

```bash
go build -o bin/ptah-ls ./cmd/ptah-ls
```

A Visual Studio Code extension that starts `ptah-ls` for Go files lives in
[`editors/vscode`](https://github.com/stokaro/ptah/tree/master/editors/vscode);
its `ptah.languageServer.path` setting points at the built binary. Any other
LSP-capable editor can run `ptah-ls` directly as a stdio language server.

Serve mode takes no arguments. `ptah-ls version` and `ptah-ls --version` print
build metadata and exit; anything else on the command line is a usage error and
exits `2`. Earlier releases silently discarded arguments and started serving
anyway, so an editor configuration that passes a document or workspace path now
fails loudly instead of appearing to work.

## Next steps

- Modeling a schema with these directives: [Go annotations](../../schema/go-annotations/).
- Declaring rows, not only structure: [Reference data](../../versioned/reference-data/).
- Checking which features your dialect supports: [Capabilities](../capabilities/).
