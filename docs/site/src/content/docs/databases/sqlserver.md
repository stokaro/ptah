---
title: SQL Server
description: SQL Server in Ptah - connection URLs, the supported T-SQL subset, collation-aware identifier comparison, and filtered indexes.
type: reference
audience:
  - "database-engineer"
readerQuestion: "Which SQL Server surface and release lines does Ptah support?"
goal: "Identify the SQL Server surface and release lines Ptah supports."
sourceOfTruth:
  - "internal/capabilityprobe/cells.go"
  - "internal/dbschema"
generated: false
searchAliases:
  - "SQL Server support"
overlaps: []
disposition: keep
sourceMode: live-database-only
owns:
  - dialect-sqlserver
---

Ptah supports SQL Server and Azure SQL as a deliberately conservative portable
subset under the canonical dialect name `sqlserver` (the aliases `mssql`,
`sql-server`, `sql_server`, and `tsql` normalize to it).
Commands that introspect this behavior require a live SQL Server database URL.

## Connecting

Pass a canonical `sqlserver://` URL; `mssql://` input is accepted and
normalized:

```bash
ptah db read --db-url "sqlserver://sa:$SA_PASSWORD@localhost:1433?database=app&encrypt=disable"
```

The Ptah-only `schema` query parameter selects the default schema for
introspection, migration metadata, and write helpers; it is removed before the
URL reaches the driver, and it defaults to `dbo`:

```bash
ptah db read --db-url "sqlserver://sa:$SA_PASSWORD@localhost:1433?database=app&schema=audit&encrypt=disable"
```

`--migrations-schema` takes precedence for migration metadata.

## Supported surface

- T-SQL rendering with bracket-quoted identifiers and schema-qualified names
  such as `dbo.users`.
- `IDENTITY(start,increment)` for auto-increment columns and
  `NVARCHAR`/`NVARCHAR(MAX)` string mapping. A change to a column's identity
  is refused by name, because `ALTER COLUMN` has no `IDENTITY`; create the
  column again instead.
  `SERIAL`, `BIGSERIAL` and `SMALLSERIAL` are written as `INT`, `BIGINT` and
  `SMALLINT` with `IDENTITY(1,1)`, unless the field gives its own SQL Server
  type. A table takes one identity column (`Msg 2744`), so a serial column
  beside another auto-increment column is refused by name. A SQL schema file
  reads `IDENTITY` and `IDENTITY(seed, increment)` as `ptah db read` prints
  them. An identity column is `NOT NULL` whether or not the file says so, and
  `NULL` on one is refused, as the server refuses it (`Msg 8147`).
- Core table DDL: primary keys, unique constraints, foreign keys, `CHECK`
  constraints, and indexes with ascending or descending key columns.
- Filtered indexes (see below).
- Views and triggers rendered from raw SQL definitions. A SQL schema file
  declares a trigger in T-SQL's own order (see [Triggers](#triggers)).
- Synonyms: declared, rendered, introspected from `sys.synonyms`, and diffed,
  including targets in another database or behind a linked server (see below).
- Extended properties at database, schema, table and column scope: declared,
  rendered, introspected from `sys.extended_properties`, and diffed (see
  below).
- Schemas the declared objects live in, created before them (see below).
- Scalar functions and procedures, with the options of their header (see
  below).
- Introspection from the `sys` catalog views.
- Transactional migration apply for DDL SQL Server supports in transactions,
  and migration-run serialization through a session application lock.
- Transactional dev-database cleanup across supported user schemas, with a
  preflight that rejects replication-enabled databases and replicated tables.
- One-row upsert rendering to `MERGE` through the Go DML AST (an embedder
  surface, not a CLI command).

SQL Server has no native enum object, so enum annotations render as
`NVARCHAR(255)` columns with a generated `CHECK` constraint:

```sql
[status] NVARCHAR(255) NOT NULL CHECK ([status] IN ('active', 'blocked'))
```

## Synonyms

A synonym is an alias for another object, and it is the one schema object whose
target may live somewhere Ptah does not manage:

```go
//ptah:schema:synonym name="current_orders" schema="app" target="sales.orders"
type CurrentOrdersSynonym struct{}
```

```sql
CREATE SYNONYM [app].[current_orders] FOR [sales].[orders];
```

Two parts name an object in this database, three another database, and four a
linked server. Ptah keeps the target as written, because the part count tells
SQL Server where to resolve the name.

**Ptah manages the alias and never the target.** SQL Server does not require
the target to exist, so nothing creates or drops the object a synonym points
at. A plan creates the alias after the tables, views and
routines it creates, and drops it before any of them is dropped. A target
behind a linked server with no database part, `srv..dbo.orders`, keeps its
empty part.

A changed target is planned as a drop and a create in that order, because T-SQL
has no `ALTER SYNONYM` and `CREATE SYNONYM` refuses a name that already exists.

Targets are compared with the server's own bracket quoting normalized away, so a
declared `dbo.orders` and a stored `[dbo].[orders]` are the same target.

A synonym belongs to the synonym owner and is bound to SQL Server and Oracle,
the two targets that have one. A schema rendered or planned for any other
target leaves it out. Two declarations of one synonym are refused.

Go export preserves the alias, schema, target spelling, and comment, including
schemas that declare only synonyms. An explicit scope that names a different set is refused.

## Extended properties

Go export preserves database, schema, table, and column properties, including
empty string values and declaration comments. The `value` attribute is required;
`value=""` is valid. Synonym and property export
refuse incomplete namespace or object coverage so an unread object cannot
become a planned removal through the exported file.

An extended property is a named value attached to a schema, a table, or a
column of one:

```go
//ptah:schema:extendedproperty name="retention" schema="app" table="orders" value="90d"
type OrdersRetentionProperty struct{}

//ptah:schema:extendedproperty name="classification" schema="app" table="orders" column="email" value="pii"
type OrdersEmailClassification struct{}
```

```sql
EXEC sp_addextendedproperty @name = N'retention', @value = N'90d', @level0type = N'SCHEMA', @level0name = N'app', @level1type = N'TABLE', @level1name = N'orders';
EXEC sp_addextendedproperty @name = N'classification', @value = N'pii', @level0type = N'SCHEMA', @level0name = N'app', @level1type = N'TABLE', @level1name = N'orders', @level2type = N'COLUMN', @level2name = N'email';
```

**The address is the identity.** SQL Server stores a property under a class and
up to two ids, so the same name on the database, on a schema, on a table of it,
and on a column of that table is four different properties. Which levels a
declaration sets decides its scope: **no level at all** is the database's own
property, `schema` alone is schema scope, adding `table` addresses the table,
and adding `column` addresses a column of it. A level without the one above it
is refused, because SQL Server has no level N without a level N-1.

```go
//ptah:schema:extendedproperty name="deployment_tier" value="production"
type DeploymentTierProperty struct{}
```

```sql
EXEC sp_addextendedproperty @name = N'deployment_tier', @value = N'production';
```

A database-scoped property passes no level: an empty `@level0name` would name
a schema called `""`. It is in no schema either,
so `--schema` neither selects nor removes it.

`--include` and `--exclude` name a property as `name[type=extended_property]`,
qualified by its schema, or by its name alone for a property on the database.
A property on a table or a column follows its table: a scope keeps or leaves it
with the table, on the declaration and on the database alike.

A changed value plans `sp_updateextendedproperty` rather than a drop and an
add, so the property is never absent partway through a script. A `DROP TABLE`
in a SQL schema file removes the properties on the table and its columns, as
SQL Server drops them with the table, and a scope that leaves a table out
leaves its properties out. Properties are
emitted after every object one can hang off, and dropped before the objects
they belong to.

`MS_Description` is refused by name. Ptah models it as the object's comment,
and a second owner of one value would plan against the first.

**A value Ptah cannot write back is reported and left alone.**
`sp_addextendedproperty` takes a `sql_variant`, so a property may hold an `int`
or a `date` as well as a string, and Ptah writes values as `N''` literals,
which would change a non-string value's stored type. Such a property appears in a read with its base type and
no value, and a comparison plans nothing for it in either direction.

A property on an object other than a table, such as a view, a procedure or an
index, is not read.

Realm cleanup refuses a database whose database- or schema-scoped properties
it would leave behind.

An extended property belongs to the SQL Server owner. A declaration is bound
to SQL Server, so a schema rendered or planned for another target leaves the
property out. Two declarations of one property are refused.

The owner compares, plans and reverses extended properties, and the HCL that
`schema inspect` writes carries them, as it does synonyms.

```hcl
synonym "current_orders" {
  schema = schema.app
  target = "sales.orders"
}

extended_property "MS_Description" {
  schema = schema.app
  table  = "orders"
  column = "total"
  value  = "order total including tax"
}
```

The owner is written as plain strings rather than as `table.orders` and
`column.total` references, because that is what the statement carries:
`@level1name = N'orders'` is a name, and a reference would resolve to nothing
once `--include` left the table out. A block with no `schema` is a
database-scoped property, the address that passes no level at all.

**What a document cannot name it does not drop.** YAML has no key for either
object, and a `.sql` document is written back with `CREATE SYNONYM` and
`sp_addextendedproperty` but read as a schema holding neither, so a file in
those formats could not have declared one — and reading its silence as intent
would make this loop destructive:

```bash
ptah schema inspect --db-url "$SQLSERVER_URL" --format sql > out.sql
ptah schema apply --db-url "$SQLSERVER_URL" --to file://out.sql
```

HCL and a Go schema — `//ptah:schema:synonym` and
`//ptah:schema:extendedproperty` — can both declare the two objects and claim
them, so silence in either is a request to remove.

## Schemas

A schema on SQL Server is an ordinary object inside the connected database, so a
declaration that puts a table in one is asking for the schema too. Ptah creates
it before the objects that need it:

```sql
IF SCHEMA_ID('app') IS NULL
    EXEC('CREATE SCHEMA [app]');
CREATE TABLE [app].[widget] (
  [id] INT PRIMARY KEY
);
```

SQL Server has no `CREATE SCHEMA IF NOT EXISTS`, a repeated `CREATE SCHEMA`
fails with `Msg 2714`, and `CREATE SCHEMA` must open its batch, which the `EXEC`
does.

The schemas come from the objects being added and from the `schema` blocks a
document declares, so a migration that adds nothing emits no schema statement and
a single-schema declaration emits none either. A schema a document declares and
puts nothing in is created too, and a rollback drops it with `DROP SCHEMA`, which
the server refuses while the schema holds an object. A plan never creates `dbo`,
and never drops a schema the document does not mention.

A schema's `comment` is its `MS_Description` extended property, because T-SQL has
no `COMMENT ON`:

```sql
EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'application',
  @level0type = N'SCHEMA', @level0name = N'app';
```

A plan writes it with the schema, sets it on a schema the database holds, and
replaces a different one. A declaration that names a schema and states no comment
leaves the comment as the server holds it. A rehearsal's reset takes the comment
back, on `dbo` as well, so a dev database's own comment on `dbo` does not survive
one.

A table's `comment` and a column's take the same shape, written right after
`CREATE TABLE`. `ptah db read` writes them that way as well, and a schema file
reads them back, so a description keeps them.

**MySQL, MariaDB, ClickHouse and Oracle do not do this:** a schema there is a
database, or on Oracle a user, which a migration does not create.

## Identifier collation

SQL Server compares object identifiers and routine execution principals using catalog collation rules — case,
accent, locale, kana, and width sensitivity all depend on the target catalog.
When comparing against a live database, Ptah asks SQL Server to resolve
candidate identifier equivalence under `COLLATE CATALOG_DEFAULT` and carries
that resolved snapshot through comparison, planning, checkpoint generation,
and shadow verification (shadow execution is rejected when the shadow catalog
resolves names differently).

An offline, dialect-only comparison cannot know the target collation, so it
confirms only exact-spelling identity. When it checks table, column, or index
names for conflicts, it treats two ASCII names that differ after ASCII case
folding as distinct, because no SQL Server collation makes them equal. Names that differ only in ASCII case, or
that hold a non-ASCII character, may be one name under some collation, so
planning rejects them as a possible conflict instead of guessing. This makes
offline SQL Server planning intentionally stricter than live planning. Embedders choose between the live-aware and offline
comparison APIs — see [Public Go API](../../extend/public-api/).

## Filtered indexes

Declare a filtered index with the index annotation's `condition` (or `where`)
attribute:

```go
//ptah:schema:index name="idx_active_users" fields="status" condition="status = 1"
```

The predicate renders verbatim as `CREATE INDEX ... WHERE status = 1`. SQL
Server cannot alter a predicate in place, so a changed predicate plans as
`DROP INDEX` plus `CREATE INDEX ... WHERE` with the new predicate.

SQL Server stores predicates in a canonical spelling — `status = 1` comes back
as `([status]=(1))` — and Ptah normalizes bracket quoting, parenthesized
numeric literals, case, and whitespace before deciding whether an index
changed. Rewrites beyond that spelling (such as the `N'...'` prefix on Unicode
string literals) are not reconstructed, so those predicates compare as changed
on every run. If a filtered index keeps reporting drift, write the spelling
`ptah db read` prints in the annotation.

## Primary keys nobody named

SQL Server names a primary key with no name itself, as
`PK__orders__3213E83F2B3A1E5C`, and the hash differs from one database to the next. A
later file that drops the key could name only the one its dev database held, and any
other database answers that it is not a constraint. So Ptah writes the name:
`<table>_pkey`. A name the author wrote stays. Where the table's name leaves no room
within the server's limit of 128 characters, the name is the table's cut short with a
hash of the whole name in front of the suffix.

## Unique constraints and checks nobody named

A `UNIQUE` on the table with no name, in a SQL schema file, takes
`<table>_<columns>_key`, for the same reason as a primary key, and a `CHECK` on the table
takes `<table>_<column>_check`. Each is numbered past a name the schema holds. A `UNIQUE`
written on a column keeps the name the server gives it.

A `CHECK` name is held once per schema, across its tables. Two `CHECK`
constraints of one name, spelled the same, are refused before any statement
runs, with the error the server answers: `There is already an object named
'ck' in the database.` for two tables, and error 8168 for one. Whether `ck` and
`CK` are one name follows the database collation: the default
`SQL_Latin1_General_CP1_CI_AS` refuses the pair and `Latin1_General_100_CS_AS`
accepts it. Ptah compares the names exactly and refuses only what holds under
every collation, so a pair that differs in case reaches the server, which
decides.

## A lower scale or precision

`ALTER COLUMN` rounds each value to the new digits and succeeds: `999.95` under
`decimal(5,1)` reads `1000.0`, and `10:00:00.9876543` under `datetime2(0)` reads
`10:00:01`. `time`, `datetimeoffset` and `money` round the same way, `smalldatetime`
rounds to the minute, and `datetime` rounds to 1/300 of a second, so
`10:00:00.001` under `datetime` reads `10:00:00.000`, and `10:00:00.123` is stored as
`10:00:00.1233333`, which counts as a change. A decimal under an integer is cut:
`1234.567` under `int` reads `1234`. A `datetimeoffset` under `datetime2`, `datetime` or
`smalldatetime` keeps its clock time and drops the offset, and only the digits count.
While a row holds a value the change would alter, `ptah schema plan`,
`ptah schema apply`, `ptah migrations plan` and `ptah migrations generate` refuse the
change, and a generated migration file reads the rows again where it runs; see
[A change that rounds stored values](../../direct/plan-and-approve/#a-change-that-rounds-stored-values).

## Column defaults

SQL Server keeps a column default as a constraint of its own, so `ALTER
COLUMN` cannot set or drop it. Ptah creates a default without a name, in
`CREATE TABLE` and in a migration alike, so the server names it, and the name
differs from one database to the next. A migration therefore does not write the
name down: it reads it from `sys.default_constraints` when it runs. This
statement replaces the default of `users.status`:

```sql
EXEC sp_executesql N'DECLARE @drop nvarchar(max) = (SELECT N''ALTER TABLE [dbo].[users] DROP CONSTRAINT '' + QUOTENAME(dc.name) FROM sys.default_constraints AS dc WHERE dc.parent_object_id = OBJECT_ID(N''[dbo].[users]'') AND dc.parent_column_id = COLUMNPROPERTY(dc.parent_object_id, N''status'', ''ColumnId'')); EXEC (@drop); ALTER TABLE [dbo].[users] ADD DEFAULT ''active'' FOR [status];';
```

It drops the current default, if the column has one, and adds the declared
default. It is one statement because the constraint name is held in a variable,
and a variable does not outlive its batch. A removed default is the same
statement without the `ADD DEFAULT`. The safety report treats the drop as a
default change rather than as the removal of a constraint that protects data:
a removed default is a warning, and a replaced one is safe.

For temporal columns, the comparison ignores case in `GETDATE()` and
`SYSDATETIME()`. It treats `CURRENT_TIMESTAMP` as `GETDATE()` because both
return the same value. `SYSDATETIME()` remains distinct because it returns
more fractional-second digits.

A default that reads a sequence, `NEXT VALUE FOR seq`, is kept as an
expression, with or without parentheses, a schema or brackets;
`'NEXT VALUE FOR seq'` stays a string. SQL Server stores the clause in upper
case and each name bracketed: `next value for dbo.seq` is stored as
`(NEXT VALUE FOR [dbo].[seq])`. The comparison reads each reference by the
names it holds, so a second apply plans nothing. It compares those names as
the database's collation does: under the default,
`SQL_Latin1_General_CP1_CI_AS`, `NEXT VALUE FOR SEQ` and a stored
`NEXT VALUE FOR [seq]` read one sequence, and under a case-sensitive collation
they differ. A schema is part of the reference, as SQL Server keeps it:
`NEXT VALUE FOR dbo.seq` and `NEXT VALUE FOR seq` differ.

SQL Server refuses to change the data type of a column that has a default, so
a plan that changes the type drops the default first and sets it again after
the `ALTER COLUMN`. It does this for a longer length of the same type too,
which the server would accept with the default in place. A nullability change
leaves the default where it is.

A temporal column's precision is compared. `datetime2`, `datetimeoffset` and `time`
hold seven fractional-second digits where a column declares none, so `datetime2` and
`datetime2(7)` are one type and `datetime2(3)` is another. A plan that lowers a
precision, or moves a column to `datetime`, `smalldatetime` or `date`, is rated
destructive, since the server rounds the digits away without a word. See
[how a column change is rated](../../direct/plan-and-approve/#how-a-column-change-is-rated).

A column's size is compared by what it holds. `(max)` has no limit, so `nvarchar(max)`
declared `nvarchar(50)` plans an `ALTER COLUMN` rated destructive, and the server
answers `2628` and keeps the column if a value is too long. `varchar`, `nvarchar`,
`char`, `nchar`, `binary` and `varbinary` hold one where a native column declares no length,
`decimal` and `numeric` are `decimal(18,0)`, and `float` is `float(53)`. So each of those
plans nothing against the size it stands for, and `decimal(10,2)` declared `decimal`
rounds `1.50` to `2`.

Lower capacities are rated destructive on the same terms: `varbinary(max)` declared
`varbinary(50)`, `image` or `ntext` under a length, `money` declared `smallmoney`, and
`float` declared `real`, where the server keeps seven digits of `1.23456789012` without a
word.

A column declared with a portable name is compared with the type that name writes.
`timestamp` is `datetime2`, `timestamptz` is `datetimeoffset`, `boolean` is `bit`, `text`
and `citext` are `nvarchar(max)`, `bytea` and `blob` are `varbinary(max)`, `char` is
`nchar`, and a bare `varchar` is `nvarchar(max)` regardless of case. A `varchar(n)`
declaration writes `nvarchar(n)`. A declared timestamp precision is preserved:
`timestamp(3)` writes `datetime2(3)`, while `timestamptz(3)` and
`timestamp(3) with time zone` write `datetimeoffset(3)`. Each plans nothing
against the type it writes.

A native `sql("VARCHAR(50)")` declaration differs from an `NVARCHAR(50)` column.
An `NVARCHAR(50)` declaration differs from a `VARCHAR(50)` column. The portable
`VARCHAR(50)` declaration still writes `NVARCHAR(50)`.

A word the server also takes as a type of its own matches the column it builds from that word too:
`text` over `text`, `char(5)` over `char(5)`, and `varchar(50)` over `varchar(50)`. Any
other column plans a change: `varchar(max)` declared `text`, `tinyint` declared `boolean`,
and `datetime2(3)` declared `timestamp`, which writes `datetime2`.
An integer-to-`bit` change is rated destructive because values outside
`0` and `1` collapse. The `datetime2(3)` change gains precision and is rated
as a warning.

`ptah migrations lint` reports `ALTER COLUMN c datetime2(0) NULL` as `data-dependent/alter-column-type`, with or
without brackets around the names, since SQL Server names the new type with no word
before it. That clause has to name a type to change a nullability too, so under
`--dev-url` a clause that names the type the column holds is not reported, and `data-dependent/set-not-null`
speaks for the `NOT NULL`. Without a dev database every such clause is reported.

## Computed columns

A generated column is a SQL Server computed column, and SQL Server either
stores it or computes it when it is read. A column declared `STORED`, written
`stored`, or declared with no kind is stored, as `AS (expr) PERSISTED`. A
column declared `VIRTUAL`, or `stored="false"`, is computed on read, as
`AS (expr)`. `ptah db read` and `ptah introspect` describe each column with
the kind it has, and `ptah-compat schema inspect` writes it as the `type` of
the `as` block, `PERSISTED` or `VIRTUAL`. A SQL schema file reads `AS (expr)`
as computed on read and `AS (expr) PERSISTED` as stored.

SQL Server stores the expression rewritten: `amount + 1` is stored as
`([amount]+(1))`. A comparison against a connected database creates the
declared column in a temporary table, reads back how the server stores it,
and compares the two stored forms. The type and nullability of a computed
column are the ones SQL Server derives from the expression, so Ptah writes
neither and compares neither.

The temporary table is created inside a transaction that is rolled back, and a
CHECK is asked the same way. T-SQL needs no semicolon between two statements,
so an expression that closed the parentheses around it could end the
transaction, and what it ran after a `COMMIT` would stay. An expression is
therefore not asked when it leaves a string, a quoted name or a comment open,
closes a parenthesis it does not open, leaves one open, or holds a semicolon
outside a string. A block comment holds the block comments inside it, as SQL
Server reads it. Such an expression is compared as written, and the run prints
a warning that names the column or the constraint and the reason. After each
probe Ptah reads `CURRENT_TRANSACTION_ID()` and `@@TRANCOUNT`, and a command
whose probe ended the transaction stops with an error.

SQL Server stores only a deterministic expression. A stored column that reads
`GETDATE()` is refused when the migration runs, with `Computed column 'seen'
in table 'events' cannot be persisted because the column is non-deterministic.
(4936)`, and the migration leaves no table behind. Declare such a column
`VIRTUAL`.

## Routines

A function or a procedure is written `CREATE OR ALTER`. Its header's `WITH`
list states how it runs, and `sys.sql_modules` reports each option back:

| Header | `sys.sql_modules` | Ptah |
| --- | --- | --- |
| none, or `EXECUTE AS CALLER` | `execute_as_principal_id` `NULL` | security `INVOKER` |
| `EXECUTE AS OWNER` | `execute_as_principal_id` -2 | security `DEFINER` |
| `EXECUTE AS 'auditor'` | `execute_as_principal_id` the user's id | security `DEFINER`, principal `auditor` |
| `SCHEMABINDING` | `is_schema_bound` 1 | kept |
| `RETURNS NULL ON NULL INPUT` | `null_on_null_input` 1 | kept |
| `RECOMPILE` | `is_recompiled` 1 | kept |

Parameter declarations accept the optional `AS`: `@amount AS DECIMAL(10, 2)`
and `@amount DECIMAL(10,2)` compare as the same parameter. Parameter names, order, and type dimensions remain significant.

The options Ptah keeps are read from a SQL schema file and from a connected
database, compared, and planned. A routine whose options change is replaced
with the declared `WITH` list, and the rollback writes the options it held. A
SQL schema file states every option a routine has, so a routine declared there
without `SCHEMABINDING` loses it. A Go annotation and an Atlas HCL block have
no way to state these options, so a routine declared there keeps the ones it
has. A plan that would replace such a routine is refused, because the
replacement would drop them.

`EXECUTE AS SELF` is refused when a SQL schema file is read. It means the
account that runs the `CREATE`, which can differ each time the schema is
applied. Write `EXECUTE AS 'name'` with that user instead.

Every other option is refused by name, because Ptah cannot write it and compare
it back: `ENCRYPTION`, `NATIVE_COMPILATION`, `INLINE` and `FOR REPLICATION`
among them. A SQL schema file that writes one is refused when it is read, and
so is a database holding a routine created with `ENCRYPTION`,
`NATIVE_COMPILATION` or an explicit `INLINE` setting, `INLINE = OFF` included;
the refusal names the routine. `CALLED ON NULL INPUT`, the default, is accepted.

T-SQL has no volatility clause. SQL Server judges a function deterministic
from its body and `SCHEMABINDING`, and no declaration states that verdict, so
the reader reports every routine `VOLATILE`.

A routine replaced for a new body is dropped first, and the plan grants back
what each grantee held on it and denies again what was denied. Measured on SQL
Server 2025.

## Scripts and batches

A SQL Server script runs in batches, and `GO` on a line of its own ends one.
The body of a trigger, a procedure or a function runs to the end of its batch,
and `CREATE TRIGGER`, `PROCEDURE`, `FUNCTION` and `VIEW` must each open a
batch. Ptah runs every statement as a batch of its own, and every SQL Server
script it writes says so with a `GO` after each statement: `ptah db read`,
`ptah schema render`, `ptah migrations generate`, and the `sql` helper of
`ptah-compat migrate diff`. Such a script runs the same way in `sqlcmd` and in
`ptah migrations up`.

```sql
CREATE TABLE [t] (
  [id] INT PRIMARY KEY
);
GO

CREATE TRIGGER [trg] ON [t] AFTER INSERT AS SET NOCOUNT ON;
GO
```

Without the `GO` after it, the trigger above would take every statement after
it into its body. A migration file of a layout another tool reads, such as
goose or golang-migrate, takes no `GO`, because such a tool may send the line
to the server as written.

Ptah reads `GO` wherever it reads a SQL Server script: the migrator, the
schema-file parser and `ptah migrations lint` each end a statement at a `GO`
line. An optional repeat count and a comment may follow it on the line. The
`GO` line belongs to no statement, so a `-- ptah:nolint` directive goes after
it, directly above the statement it suppresses.

## Views that read other views

SQL Server checks a view's query when the view is created, so Ptah creates each view
after the views it reads and refuses views that read one another in a ring;
[Views that read other views](../mysql/#views-that-read-other-views)
on the MySQL page says what counts as a read.

## Triggers

A SQL schema file declares a DML trigger with its table before its timing:

```text
CREATE [OR ALTER] TRIGGER [schema.]name ON table [WITH EXECUTE AS CALLER]
  {FOR | AFTER | INSTEAD OF} {INSERT | UPDATE | DELETE} [, ...]
  AS body
```

`FOR` and `AFTER` are one timing, which `sys.triggers` records as `AFTER`, so
either compares equal to the database. Each event is written once, and the
events are separated by commas. A trigger fires once per statement, as every
SQL Server DML trigger does. A schema in the trigger's name must be the schema
of its table, and an unqualified table is in `dbo`; SQL Server refuses any
other schema with `Msg 2103`. The table may be a view: an `INSTEAD OF` trigger
is how a view takes writes, and `ptah db read` describes one on a view as it
describes one on a table.

The body follows `AS` and runs to the end of its batch. Ptah ends it at a `GO`
line, at the `END` that closes a `BEGIN ... END` body, or at the end of the
file. A body of several statements outside a `BEGIN ... END` block needs a
`GO` line after it when anything follows it.

A header clause that Ptah does not model is refused by name rather than
dropped, because a trigger read without the clause would compare equal to one
that has it. Measured on SQL Server 2025:

| Clause | What SQL Server records | Ptah |
| --- | --- | --- |
| `WITH EXECUTE AS CALLER` | `execute_as_principal_id` `NULL`, as for no clause | read |
| another `WITH EXECUTE AS` | `execute_as_principal_id` -2 or a user's id | refused |
| `WITH ENCRYPTION` | no definition: `OBJECT_DEFINITION` is `NULL` | refused |
| `WITH NATIVE_COMPILATION` or `SCHEMABINDING` | a trigger on a memory-optimized table | refused |
| `NOT FOR REPLICATION` | `is_not_for_replication` 1 | refused |
| `ON DATABASE` or `ON ALL SERVER` | a DDL or logon trigger | refused |

What SQL Server itself refuses is refused as well: `IF NOT EXISTS`,
`OR REPLACE`, `BEFORE`, `UPDATE OF`, `TRUNCATE`, events joined by `OR`, an
event written twice, and `WITH APPEND`, which SQL Server 2025 answers with
`Msg 195`.

## Granting and revoking every permission

`GRANT ALL ON t TO r` grants `DELETE`, `INSERT`, `REFERENCES`, `SELECT` and
`UPDATE` on a table, a view or a table-valued function, `EXECUTE` on a
procedure, and `EXECUTE` and `REFERENCES` on a scalar function. A role holding
the list for its object holds a declared `ALL`, and a role the schema creates
loses every other permission, `CONTROL` included.

`REVOKE ALL ON t FROM r` takes every permission the role holds on `t` and its
columns. SQL Server's own `REVOKE ALL` leaves `ALTER`, `CONTROL` and
`VIEW DEFINITION`, through which a member still reads and drops the table.
Ownership of `t` or its schema, a permission on the schema such as `CONTROL`,
and a role such as `db_datareader` still reach `t`.

Permissions of users, application roles and `public` on the schema's objects
are read, as a role's are. Measured on SQL Server 2025:

- `ptah db read` and `ptah schema inspect --format sql` write their grants and
  their `DENY`s, but do not create the user or the application role. See
  [Denying a permission](#denying-a-permission).
- `ptah schema security` reports them: a grant to `public` raises the warning
  `PRV03`, and a grant on a routine raises `PRV01`, so `--fail-on any` can
  fail on a database it passed.
- `ptah schema diff` between two databases reads `--to` as complete, so it
  revokes a grant of a user or of `public` that `--to` lacks, and takes away a
  `DENY` that `--to` lacks.

## Permissions on a schema

`GRANT SELECT, VIEW DEFINITION ON SCHEMA::dbo TO reader` names the schema
after its class and `::`, and a SQL schema file reads it the same way. A
permission named in more than one word, such as `VIEW DEFINITION`,
`TAKE OWNERSHIP` or `VIEW CHANGE TRACKING`, is read as one permission, on a
schema and on an object alike. So what `ptah db read` writes for them reads
back, and a plan against the database it came from is empty. Measured on SQL
Server 2025.

## Denying a permission

`DENY SELECT ON dbo.t TO reader` withholds a permission from the grantee and
every member of it, whatever grant on the schema or membership gives it. A SQL
schema file declares one on an object, on columns (`DENY SELECT (secret) ON
dbo.t TO reader`) or on a schema (`DENY DELETE ON SCHEMA::app TO reader`). The
reader reads each row of `sys.database_permissions` with state `D`, so
`ptah db read` writes it, and its output applied to another database denies
what the source denies.

The server keeps one row for a permission of one grantee on one securable, and
a statement on a whole object reaches the rows of the permission on its
columns. A schema file, and a schema directory read file by file, follow the
same order, measured on SQL Server 2025:

- a `DENY` takes the place of a `GRANT` before it, and a `GRANT` or a `REVOKE`
  after it takes the `DENY`;
- a `DENY` on a table takes the column grants and column denies of the
  permission, and a `GRANT` or a `REVOKE` on a table takes its column denies;
- a column grant after a `DENY` on its table keeps both rows, and a member
  reads that column;
- a column `DENY` after a grant on its table keeps both rows, and a member
  reads every other column;
- a column `DENY` under a `DENY` on its table adds nothing.

So a schema that keeps a column grant under a `DENY` on its table writes the
`GRANT` after the `DENY`, and one that denies a column under a grant on its
table writes the `DENY` after the `GRANT`. A plan runs a `DENY` on an object
before the grants it adds and a `DENY` on a column after them. After a `DENY`
on a table it grants again each column grant the schema declares, and nothing
else: a grant the database holds and the schema does not declare stays
taken. Where a plan grants or revokes a permission on a whole table, it
denies again each column `DENY` of that permission that the database holds
and the plan keeps. A revoke of the grant option alone leaves the column
`DENY` in place.

A `DENY` of a permission the grantee holds `WITH GRANT OPTION`, on the object
or on a column a `DENY` on the object reaches, carries `CASCADE`, because the
server answers `Msg 4611` without it. A column `DENY` carries it too where the
plan grants the option before it, on that column or on the whole object.
`CASCADE` also takes the permission from
every principal the grantee granted it to on the whole object or on the denied
column, and leaves a grant on another column. So a plan where the schema keeps
a grant `CASCADE` takes is refused:

```text
sqlserver: cannot deny SELECT on dbo.t to "r": "r" holds it WITH GRANT OPTION, so SQL Server denies it with CASCADE, which also takes the SELECT that "r" granted to "r_v" (name), and the schema does not revoke those grants; revoke them first, or declare their REVOKE in the schema
```

A schema file is refused where it writes `DENY ... CASCADE`, `DENY ... AS`,
`DENY ALL`, or a permission it also grants or revokes to the same grantee.
Every other target refuses a `DENY` by name.

`PUBLIC` without brackets, in any case, and `[public]` name the fixed role
`public`, the principal with `principal_id` 0. The read names that principal
`PUBLIC` too, and a plan writes it as `[public]`. Measured on SQL Server 2025,
a case-sensitive database refuses `[PUBLIC]` with `Msg 15151` unless a role of
that name exists, and it accepts a role named `[Public]` beside `public`, which
stays a role of its own. On a case-insensitive database `[Public]`, and every
bracketed spelling but `[public]` and `[PUBLIC]`, reaches `public` as well, but
a schema file records it as a role named `Public`, so a plan never settles
there; write `PUBLIC` or `[public]`. A database that holds a role or a user named `PUBLIC`,
spaces aside, is refused before anything runs, because its grants could not be
told from those of `public`:

```text
sqlserver: read roles: unsupported feature: the database principal "PUBLIC" has the name a schema gives the fixed role public, so a grant to one cannot be told from a grant to the other; rename the principal
```

A principal whose name starts with a space, such as `[ public]`, is refused
the same way, in the database and as a grantee in a schema file. A grant does
not keep that white space, so a statement for the principal would address
another one. The server ignores trailing spaces when it compares names, so
`[r ]` names `r` in the database and in a schema file, and is read as `r`.

A `DENY` the database holds and the schema leaves out stays, and the run names
it on standard error:

```text
UNMANAGED: the database denies SELECT on TABLE dbo.t (secret) to "reader", and the schema does not declare the DENY, so the plan does not revoke it; declare it to manage it, or revoke it by hand
```

A Go annotation, a YAML document and an HCL document cannot declare one, so
taking it away would give the role what an administrator denied it. A
comparison of two databases reads the desired side as complete, so it takes
away a `DENY` that side lacks, as it revokes a grant. A column `DENY` under a
grant on its table that the plan keeps goes with a `GRANT` on the column,
because a `REVOKE` there leaves a row that still withholds the column.
`ptah introspect` refuses a database that holds a `DENY`, because no
annotation writes one, and `ptah schema inspect --format hcl` leaves it out
and reports it. The plan check and the drift report rate a `DENY` added or
taken away a warning.

## Permissions on columns

`GRANT SELECT (id, name) ON dbo.t TO reader` limits a permission to columns
of a table or a view. `SELECT`, `UPDATE`, `REFERENCES` and `UNMASK` take a
column list, `UNMASK` from SQL Server 2022 on. Any other permission with one
is refused by name, as the server refuses it with `Msg 1020`.

A table permission replaces the column rows of the same permission, and a
column revoke under one withholds the column, so a plan runs every revoke
before any grant. A table permission with a column revoked under it, such as
by `REVOKE SELECT (secret)`, reads as the permission on each other column. It
also covers a column added later: measured on SQL Server 2025, a member reads
that column until the next plan revokes it, and nothing revokes it from a role
the schema does not create. A table permission with a column `DENY` under it
reads as the permission on the table and the `DENY`; see
[Denying a permission](#denying-a-permission).

A grantable permission loses its option first, with `REVOKE GRANT OPTION FOR
... CASCADE`, because the server refuses the revoke otherwise (`Msg 4611`).
`CASCADE` also revokes the permission from every principal the role granted it
to, for every permission held with the option, `CONTROL` and `ALTER` included.
Column names compare under the database's
[collation](#identifier-collation). Measured on SQL Server 2019, 2022 and 2025.

So a plan refuses to revoke a grant option, or a grantable permission, where
the grantee has granted the permission onward and the schema does not revoke
that grant. The server keeps a row for each grantor, and the refusal names each
principal holding a row the grantee granted:

```text
sqlserver: cannot revoke the grant option for SELECT on dbo.t1 from "r": SQL Server revokes it with CASCADE, which also takes the SELECT that "r" granted to "r_v", and the schema does not revoke those grants; revoke them first, or declare their REVOKE in the schema
```

This holds for a `REVOKE GRANT OPTION FOR` in a SQL schema file and for a role
the schema creates that it declares without the option. A grant `r_v` holds
from another grantor stays, and with no onward grant `CASCADE` takes nothing
more. Measured on SQL Server 2025.

## Limitations

- No PostgreSQL-style extensions and no materialized views.
- Row-level security is a `SECURITY POLICY`, declared by a Go row-level
  security annotation scoped to SQL Server. Its `USING` is a filter predicate
  and its `WITH CHECK` a block predicate, each a call of a two-part inline
  table-valued function, and `FOR INSERT`, `UPDATE` or `DELETE` names the
  block predicate's operation. A declaration T-SQL has no form for, such as
  one with a `TO` role list, an inline expression, or a table switch, is
  refused when the schema is read, because SQL Server scopes a predicate
  inside the function body. SQL Server stores some predicate arguments
  rewritten, such as a `CAST`; a comparison against a connected database asks
  the server how it stores the declaration.
- Column drift planning handles type, nullability, and default changes (see
  [Column defaults](#column-defaults)). A change of a computed column's
  expression or kind, and unique and `CHECK` changes, need a manual
  migration; the plan refuses them by name.
- Automatic column removal is rejected, because dependent constraints,
  defaults, and indexes must be dropped in the correct order first.
- Routines have no overloads. A schema that declares two functions, or two
  procedures, of one name is refused before any statement is planned: under
  `CREATE OR ALTER` the second replaces the first without an error, measured
  on SQL Server 2022.
- Linked servers are not managed. A linked server belongs to the whole
  instance, so a schema for one database cannot own or drop it without
  changing the others. It also opens a path to another server, which is why
  a replay against a dev database refuses `sp_addlinkedserver`. Its remote
  login holds a password the server never returns, so a plan could not tell a
  changed password from an unchanged one. A SQL schema file that runs `EXEC
  sp_addlinkedserver` is refused (`unsupported SQL statement: EXEC`), a
  `linked_server` block in an HCL schema is refused, and `ptah-compat` reports
  the block as ignored. Define the linked server where the instance is
  provisioned. A synonym may still name an object behind one (see
  [Synonyms](#synonyms)).
- Ptah does not validate a synonym's target: SQL Server creates a synonym
  naming a missing object, and the synonym fails when something uses it.
- View and trigger introspection records the persisted definition text without
  normalizing it into drift-safe definitions.
- Index planning preserves key order, direction, filtered predicates, and
  `INCLUDE` columns in the order written. A change to the included columns
  rebuilds the index with `DROP INDEX` and `CREATE INDEX`.
- Engine-specific options such as `WITH (ONLINE = ON)` are not planned.
- Dev-database cleanup rejects database replication, replicated tables, and
  unsupported database-scoped artifacts before its first DDL statement.
- A column declared both `PRIMARY KEY` and `UNIQUE` is refused rather than
  rendered. SQL Server rejects `a INT PRIMARY KEY UNIQUE`, and emitting the key
  alone would discard what the author wrote. Write the key alone, or write
  `a INT UNIQUE, PRIMARY KEY (a)`, which Ptah renders as the key and a unique
  constraint.

## Next steps

- Comparing engine depth before committing: [Database support matrix](../support-matrix/).
- Declaring indexes and constraints: [Go annotation reference](../../reference/go-annotations/).
- Gating destructive changes on any engine: [Lint and gate unsafe SQL](../../versioned/lint/).
