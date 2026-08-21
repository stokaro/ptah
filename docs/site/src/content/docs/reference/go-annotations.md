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
| [`ptah:schema:procedure`](#ptahschemaprocedure) | A stored procedure | struct |
| [`ptah:schema:trigger`](#ptahschematrigger) | A database trigger | struct |
| [`ptah:schema:view`](#ptahschemaview) | A database view | struct |
| [`ptah:schema:matview`](#ptahschemamatview) | A materialized view | struct |
| [`ptah:schema:role`](#ptahschemarole) | A database role | struct |
| [`ptah:schema:grant`](#ptahschemagrant) | Database grants | struct |
| [`ptah:schema:rls:enable`](#ptahschemarlsenable) | Row-level security enablement | file or struct |
| [`ptah:schema:rls:policy`](#ptahschemarlspolicy) | A row-level security policy | file or struct |
| [`ptah:schema:data`](#ptahschemadata) | Reference/seed row data for a table | struct |

Placement is semantic, not cosmetic. A struct directive belongs in the doc
comment of a Go struct declaration; a field directive belongs in the doc comment
of the field it describes. File-scoped RLS directives may use a separate comment
group, but they still need enough attributes to resolve to a parsed RLS object.
`ptah schema export --cleanup-go-annotations` refuses to remove any recognized
standalone directive that does not meet those conditions. Directive names use an
exact token boundary: `//ptah:schema:tableau` is an ordinary comment, not a table
directive.

## Scoping an object to dialects

Every directive that declares a standalone database object accepts `dialects`, a
comma-separated list of the targets the object belongs to:

```go
//ptah:schema:function name="get_current_tenant_id" returns="TEXT" language="plpgsql" dialects="postgres,cockroachdb,yugabytedb" body="BEGIN RETURN current_setting('app.tenant_id', true); END;"
type CurrentTenant struct{}
```

An object whose `dialects` excludes the target is **absent** from that target's
desired state. It is not skipped and not refused: nothing compares it, nothing
plans it, and `ptah schema render` prints a note on stderr naming what was left
out and which dialects it was declared for.

Absence is what makes a shared schema converge. Without a scope, a
PostgreSQL-only object on a MySQL target is either passed over with a comment —
in which case `schema apply` exits 0 having created nothing and the next
comparison asks for the same object again, forever — or refused outright, which
makes one schema across `postgres`, `mysql` and `mariadb` impossible.

Rules:

- **Omitting `dialects` means every dialect.** Declarations written before this
  attribute existed are unaffected, and a scope can only narrow an object, never
  widen one.
- **Every accepted spelling resolves to the same target.** `dialects="postgresql"`
  and `dialects="postgres"` select the same dialect.
- **A dialect family is not implied.** `dialects="postgres"` does not include
  `cockroachdb`, `yugabytedb` or `spanner`; name each target you mean.
- **A scope that names no supported dialect is a parse error**, including an
  empty `dialects=""`. Reading a typo as "belongs to nothing" would drop the
  object from every target with every command still exiting 0.
- **`ptah schema export --to hcl` reports the scope as an export loss.** Atlas
  HCL cannot carry it, so `--cleanup-go-annotations` refuses to delete an
  annotation whose scope the exported file would not preserve.

`dialects` is accepted on `extension`, `sequence`, `domain`, `composite`,
`range`, `function`, `trigger`, `view`, `matview`, `role`, `grant`,
`rls:enable` and `rls:policy`. Directives that describe table structure —
`table`, `field`, `index`, `constraint`, `embedded`, `enum` and `schema` — do
not accept it.

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
| `ttl_delete_batch_size` | No | CockroachDB row-level TTL: rows deleted per batch; at least 1. |
| `ttl_delete_rate_limit` | No | CockroachDB row-level TTL: rows deleted per second; at least 1. |
| `ttl_disable_changefeed_replication` | No | CockroachDB row-level TTL: omits the job's deletes from changefeeds. `true`/`false`. |
| `ttl_expiration_expression` | No | CockroachDB row-level TTL: SQL expression whose value is when a row expires. Enables the TTL. |
| `ttl_expire_after` | No | CockroachDB row-level TTL: interval after a row is written at which it expires, such as `3 days`. Enables the TTL. |
| `ttl_job_cron` | No | CockroachDB row-level TTL: cron schedule for the deletion job. |
| `ttl_label_metrics` | No | CockroachDB row-level TTL: labels the job's metrics with the table name. `true`/`false`. |
| `ttl_pause` | No | CockroachDB row-level TTL: pauses the deletion job without removing the policy. `true`/`false`. |
| `ttl_select_batch_size` | No | CockroachDB row-level TTL: rows selected per batch; at least 1. |
| `ttl_select_rate_limit` | No | CockroachDB row-level TTL: rows selected per second; at least 1. |

The `ttl_` attributes declare CockroachDB row-level TTL and are refused on every
other dialect before anything is applied. Either `ttl_expiration_expression` or
`ttl_expire_after` enables the policy, and the rest are refused without one. One
real CockroachDB parameter is deliberately absent — `ttl_row_stats_poll_interval`
— because the server rewrites the duration it stores and drops a value below one
second entirely, so a declaration could never read back as written; declaring it
is refused by name with the reason.
See [CockroachDB row-level TTL](../../databases/support-matrix/#cockroachdb-row-level-ttl).

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
| `include` | No | Comma-separated INCLUDE columns for PostgreSQL, YugabyteDB, or the Spanner PostgreSQL dialect. Order is preserved. |
| `name` | No | Index name. |
| `nulls_distinct` | No | Controls NULLS DISTINCT behavior where supported. `true`/`false`. |
| `ops` | No | PostgreSQL operator class. |
| `table` | No | Explicit target table. |
| `type` | No | Index type or method. |
| `unique` | No | Creates a unique index. `true`/`false`; bare form allowed. |
| `where` | No | Atlas-style partial index condition alias. |

Omit `include` when the index has no payload columns. A present value with any
empty element fails parsing instead of silently removing that element. This
includes empty, whitespace-only, comma-only, sparse, and trailing-comma lists.

The accepted access methods depend on the target: PostgreSQL accepts the
default, `BTREE`, and `GIST`, plus `SPGIST` on PostgreSQL 14 and newer;
YugabyteDB accepts the default and `LSM`, with `BTREE` normalized to its
documented default-LSM alias; and the Spanner PostgreSQL dialect accepts only
the default. CockroachDB and every other dialect reject `include` before
emitting SQL.

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
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `name` | Yes | Domain name. |
| `not_null` | No | Marks the domain NOT NULL. `true`/`false`. |
| `schema` | No | Target schema/namespace. |
| `type` | Yes | Underlying base data type. |

### `//ptah:schema:composite`

Declares a PostgreSQL composite type.

| Attribute | Required | Description |
| --- | --- | --- |
| `comment` | No | Composite type comment. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
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
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
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
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `if_not_exists` | No | Adds IF NOT EXISTS where supported. `true`/`false`. |
| `name` | No | Extension name. |
| `schema` | No | PostgreSQL installation schema. Empty means the target's default schema. |
| `version` | No | Extension version. |

### `//ptah:schema:sequence`

Declares a standalone PostgreSQL sequence.

| Attribute | Required | Description |
| --- | --- | --- |
| `as` | No | Underlying integer type, such as bigint. |
| `cache` | No | CACHE size. |
| `comment` | No | Sequence comment. |
| `cycle` | No | Enables CYCLE wrap-around. `true`/`false`. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
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
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `language` | No | Function language. |
| `name` | No | Function name. |
| `params` | No | Function parameter list. |
| `returns` | No | Return type. |
| `security` | No | Security mode, such as DEFINER. |
| `volatility` | No | Volatility class. |

### `//ptah:schema:procedure`

Declares a stored procedure: a routine that returns nothing and is invoked with `CALL`.

A procedure is the same catalog object as a function with one property removed, so it takes
the same attributes minus `returns`. Declaring `returns` is refused rather than ignored:
`CREATE PROCEDURE ... RETURNS` does not parse on either engine that has procedures, and
accepting the attribute would mean a declaration that says one thing while the database holds
another.

```go
//ptah:schema:procedure name="archive_tenant" params="tenant_id integer" language="sql" body="DELETE FROM tenants WHERE id = tenant_id"
type ArchiveTenant struct{}
```

| Attribute | Required | Description |
| --- | --- | --- |
| `body` | No | Procedure body SQL. |
| `comment` | No | Procedure comment. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `language` | No | Procedure language. |
| `name` | No | Procedure name. |
| `params` | No | Procedure parameter list. |
| `security` | No | Security mode, such as DEFINER. |
| `volatility` | No | Volatility class. |

### `//ptah:schema:trigger`

Declares a database trigger.

| Attribute | Required | Description |
| --- | --- | --- |
| `body` | Yes | Trigger body SQL. |
| `comment` | No | Trigger comment. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
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
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `name` | Yes | View name. |
| `with_check` | No | Controls WITH CHECK OPTION where supported. `true`/`false`. |

### `//ptah:schema:matview`

Declares a materialized view.

| Attribute | Required | Description |
| --- | --- | --- |
| `body` | Yes | Materialized view SELECT body. |
| `comment` | No | Materialized view comment. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `name` | Yes | Materialized view name. |
| `refresh` | No | ClickHouse refresh schedule, as ClickHouse spells it. See below. |

`refresh` carries a ClickHouse **scheduled** materialized view's schedule, in
the engine's own words: `every 1 hour`, `after 30 minute`,
`every 1 day offset 2 hour randomize for 30 minute append`. Omitting it leaves
an ordinary materialized view, maintained by inserts into its source.

The server rewrites intervals — `every 60 minute` is stored as `EVERY 1 HOUR` —
so the declaration is normalized to the stored spelling when it is parsed. Any
spelling of the same schedule converges. A schedule the server would refuse is
refused here instead: an interval mixing calendar units with clock units, a
zero interval, or `offset` on an `after` schedule.

It is a ClickHouse property and only that. It is not the retired
cross-dialect strategy below, which described an operation rather than state:

`refresh_strategy` is not an attribute. Ptah does not refresh materialized
views: one is populated when it is created, a changed `body` is reconciled as a
drop and a create that populates it again, and it goes stale only when its
source data changes, which schema reconciliation cannot observe. Refresh from
your own scheduler.

An annotation that still declares it is refused while the source file is
parsed, with that reason, on every dialect -- including the bare form with no
value, and including a view scoped away from the current target by `dialects`.
The name stays recognized so the refusal explains itself instead of reading as
a misspelling.

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
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `inherit` | No | Controls role inheritance; defaults to true. `true`/`false`. |
| `login` | No | Creates the role with LOGIN. `true`/`false`. |
| `name` | No | Role name. |
| `password` | No | Role password. |
| `replication` | No | Allows replication. `true`/`false`. |
| `superuser` | No | Creates the role as SUPERUSER. `true`/`false`. |

Most of these attributes are PostgreSQL-family notions. A ClickHouse role
carries none of them — `system.roles` is `(name, id, storage)` — so declaring
`password`, `login`, `superuser`, `createdb`, `createrole`, or `replication`
for a ClickHouse target is refused rather than dropped, and `comment` is
emitted as a leading SQL comment because the engine cannot store one. See
[ClickHouse roles and grants](../../databases/support-matrix/#clickhouse-roles-and-grants).

### `//ptah:schema:grant`

Declares database grants.

| Attribute | Required | Description |
| --- | --- | --- |
| `comment` | No | Grant comment. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `grant_option` | No | Alias for `with_option`. `true`/`false`. |
| `on_schema` | No | Target schema. |
| `on_sequence` | No | Target sequence. |
| `on_table` | No | Target table. |
| `privilege` | No | Privilege or comma-separated privileges. |
| `privileges` | No | Alias for `privilege`. |
| `role` | No | Target role. |
| `with_option` | No | Adds WITH GRANT OPTION where supported. `true`/`false`. |

On ClickHouse a grant names one scope, and `on_table` must be qualified as
`database.table` because rendering is offline and has no current database to
resolve a bare name against. `on_sequence`, wildcard scopes and column-scoped
privileges such as `SELECT(id)` are refused, as is declaring the same privilege
on both `db.*` and `db.t` — the server would absorb the narrower grant, so the
pair could never converge. `role` must name a role the same schema declares,
because ClickHouse resolves a grantee across users and roles and a user of that
name would win. Privilege names the server rewrites on the way in — `ALL`,
`CREATE`, `DROP`, `SYSTEM` and the rest — are refused too, because they never
read back as written. See
[ClickHouse roles and grants](../../databases/support-matrix/#clickhouse-roles-and-grants).

### `//ptah:schema:rls:enable`

Enables row-level security on a table.

| Attribute | Required | Description |
| --- | --- | --- |
| `comment` | No | RLS enablement comment. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
| `table` | No | Target table. |

### `//ptah:schema:rls:policy`

Declares a row-level security policy.

| Attribute | Required | Description |
| --- | --- | --- |
| `comment` | No | Policy comment. |
| `dialects` | No | Comma-separated target dialects this object belongs to; omitted means every dialect. See [Scoping an object to dialects](#scoping-an-object-to-dialects). |
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
