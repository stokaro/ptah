# PostgreSQL User-Defined Types with Ptah

Ptah supports three PostgreSQL user-defined type objects as first-class, annotation-driven schema elements: **domains**, **composite types**, and **range types**. Each flows through the full generate / compare / migrate / rollback / introspect lifecycle.

These are PostgreSQL features Atlas keeps out of its open-source core (they exist only in the proprietary "Pro" build). Ptah provides them as open (MIT), local, no-account, embeddable capabilities — see epic [#654](https://github.com/stokaro/ptah/issues/654).

User-defined types are PostgreSQL-only. On MySQL, MariaDB, and SQLite they are not emitted (the SQLite planner rejects them explicitly).

## Domains

A domain is a base type constrained with optional `NOT NULL`, `DEFAULT`, and `CHECK` clauses.

```go
//migrator:schema:domain name="email" type="TEXT" not_null="true" check="VALUE ~ '@'"
type EmailDomain struct{}
```

Renders `CREATE DOMAIN "email" AS TEXT NOT NULL CHECK (VALUE ~ '@');`.

| Attribute | Description |
| --- | --- |
| `name` | Domain name (required) |
| `schema` | Target schema/namespace |
| `type` | Underlying base data type (required) |
| `not_null` | `true` to add `NOT NULL` |
| `default` | Literal `DEFAULT` value |
| `default_expr` | `DEFAULT` expression |
| `check` | `CHECK` expression (uses `VALUE`) |
| `comment` | Optional comment |

> Round-trip note: PostgreSQL normalizes type spellings (`VARCHAR(n)` reads back as `character varying(n)`) and `CHECK` expressions, so a domain over such a base type or with a `CHECK` may show a spurious diff on re-compare. Domains over canonical types (`TEXT`, `INTEGER`, `BIGINT`) round-trip cleanly.

## Composite types

A composite type is a structured set of named fields, usable as a column type.

```go
//migrator:schema:composite name="address" fields="street:TEXT,city:TEXT,zip:INTEGER"
type AddressType struct{}
```

Renders `CREATE TYPE "address" AS ("street" TEXT, "city" TEXT, "zip" INTEGER);`.

| Attribute | Description |
| --- | --- |
| `name` | Composite type name (required) |
| `schema` | Target schema/namespace |
| `fields` | Comma-separated `name:type` list (required) |
| `comment` | Optional comment |

## Range types

A range type describes a range of values over an ordered subtype.

```go
//migrator:schema:range name="floatrange" subtype="float8" subtype_diff="float8mi"
type FloatRange struct{}
```

Renders `CREATE TYPE "floatrange" AS RANGE (SUBTYPE = float8, SUBTYPE_DIFF = float8mi);`.

| Attribute | Description |
| --- | --- |
| `name` | Range type name (required) |
| `schema` | Target schema/namespace |
| `subtype` | Element subtype (required) |
| `subtype_opclass` | Operator class for the subtype |
| `collation` | Collation for the subtype |
| `canonical` | Canonicalization function |
| `subtype_diff` | Subtype difference function |
| `comment` | Optional comment |

Range types have no in-place `ALTER`, so a changed range is dropped and recreated, and range comparison matches by name only.

## Ordering

Ptah emits user-defined types after extensions and enums but before tables, so table columns can reference them. Within the group the order is domains → ranges → composites (composites may reference the others). Drops run after tables, and `DROP TYPE` / `DROP DOMAIN` are classified as destructive by the safety gate.
