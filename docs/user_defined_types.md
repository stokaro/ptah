# PostgreSQL User-Defined Types with Ptah

Ptah supports three PostgreSQL user-defined type objects as first-class, annotation-driven schema elements: **domains**, **composite types**, and **range types**. Each flows through the full generate / compare / migrate / rollback / introspect lifecycle.

These are PostgreSQL features Atlas keeps out of its open-source core (they exist only in the proprietary "Pro" build). Ptah provides them as open (MIT), local, no-account, embeddable capabilities — see epic [#654](https://github.com/stokaro/ptah/issues/654).

User-defined types are PostgreSQL-only. On MySQL, MariaDB, and SQLite they are not emitted (the SQLite planner rejects them explicitly).

## Domains

A domain is a base type constrained with optional `NOT NULL`, `DEFAULT`, and `CHECK` clauses.

```go
//ptah:schema:domain name="email" type="TEXT" not_null="true" check="VALUE ~ '@'"
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

> Round-trip and reconciliation notes:
> - Base types are canonicalized before comparison (`VARCHAR(n)` ↔ `character varying(n)`, `float8` ↔ `double precision`, `int4` ↔ `integer`, etc.), so a domain over any spelling round-trips cleanly.
> - `check` and `default`/`default_expr` are **create-only**: they are emitted on `CREATE DOMAIN` but not reconciled by the diff engine, because PostgreSQL rewrites `CHECK` expressions on read-back (adding parentheses and `::casts`), which a string comparison would report as a phantom change. Changing a domain's `CHECK` or `DEFAULT` after creation requires a manual migration.
> - There is no in-place `ALTER` for a base-type change, so a genuine `type` or `not_null` modification is emitted as a **non-`CASCADE`** drop + recreate. If the domain is still used by a column the drop fails loudly rather than dropping the column; reconcile such changes manually.

## Composite types

A composite type is a structured set of named fields, usable as a column type.

```go
//ptah:schema:composite name="address" fields="street:TEXT,city:TEXT,zip:INTEGER"
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
//ptah:schema:range name="floatrange" subtype="float8" subtype_diff="float8mi"
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

## Extension-owned types

Reading a PostgreSQL database describes only the domains, composite types and
range types the user declared. One that an extension owns is left to the
extension, for the same reason extension-owned functions always have been: it
is created by `CREATE EXTENSION` and cannot be created or dropped
independently.

Describing one made the description declare a type the extension already
creates, and replaying such a description failed — measured on PostgreSQL
17.10 against `CREATE EXTENSION lo`, which supplies the domain `lo`:

```text
ERROR: type "lo" already exists (SQLSTATE 42710)
SQL: CREATE DOMAIN "lo" AS oid;
```

Ownership is read from `pg_depend` (`deptype = 'e'`) rather than from the type
name, so a type of your own named closely after an extension's — `lo_own`
beside `lo` — is still described in full.

## Ordering

Ptah emits user-defined types after extensions and enums but before tables, so table columns can reference them. Within the group the order is derived from what each definition names rather than fixed by kind: a domain over a composite waits for the composite, and a composite whose field is a domain waits for the domain. Both directions occur, so no fixed order of kinds serves them — `domains → ranges → composites` emitted `CREATE DOMAIN d_comp AS addr` ahead of `CREATE TYPE addr`, and PostgreSQL has no forward declaration for a type. A reference the plan does not create, such as a built-in type or a type the database already has, adds no ordering constraint, and a cycle — which PostgreSQL refuses to create anyway — falls back to declaration order rather than dropping a type from the plan.

Enums are emitted ahead of the whole group and take no part in the ordering, because an enum names no other user-defined type.

A domain or composite that changed is dropped and recreated. The drops are ordered by what the **database currently holds**, not by the target definitions the creations follow. A `DROP` executes against the schema as it stands, so the only reference that can block it is one that schema still carries: the deliberately non-`CASCADE` drop of a composite fails while a domain there still names it, so that domain goes first.

The two orders are the same when a reference survives the change, and they are not when the change is what moves it. Reconciling a database holding `CREATE TYPE cc AS (f integer)` with `CREATE DOMAIN dd AS cc` over it against a target of `CREATE DOMAIN dd AS integer` with `CREATE TYPE cc AS (f dd)` drops `dd` before `cc` and then creates `dd` before `cc` — the reverse of one another in name only. That is not a conflict: only the current schema can refuse a `DROP`, and only the target schema describes what is being built.

Removals of types that are gone from the target schema run after tables, and `DROP TYPE` / `DROP DOMAIN` are classified as destructive by the safety gate.
