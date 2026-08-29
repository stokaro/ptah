---
title: Oracle
description: Oracle in Ptah - what renders, plans and reads back on the 23 and 21 release lines, bare identifiers, type mappings, object types, and PL/SQL routines.
---

Oracle renders, plans, connects and reads a live catalog, against two measured
release lines, 23 and 21. A schema Ptah applies reads back as itself on both:

```text
ptah schema inspect --db-url oracle://user:pass@host:1521/FREEPDB1 > live.hcl
ptah schema diff --from oracle://user:pass@host:1521/FREEPDB1 --to live.hcl
Schemas are synced, no changes to be made.
```

Comparing a **declared file** against a live Oracle catalog does not converge
yet. Tables, columns and indexes fold across the case difference between a
declaration and the catalog; constraint names do not, so an unnamed `CHECK`
declared as `orders_total_check` and stored as `ORDERS_TOTAL_CHECK` is dropped
and re-added on each apply. Tracked by
[#1875](https://github.com/stokaro/ptah/issues/1875).

## Identifiers and type mappings

Identifiers are written **bare**, which is Oracle-only among the engines here
and is forced by the engine rather than chosen. Oracle folds an unquoted name to
upper case and preserves a quoted one, so a name has two spellings, and a
declaration has to agree with every expression that references it. A `CHECK` or
a generated expression is author text Ptah does not rewrite, so the declaration
is what moves:

```sql
CREATE TABLE q ("view_count" NUMBER(10) CHECK (view_count >= 0))   -- refused
CREATE TABLE b (view_count NUMBER(10) CHECK (view_count >= 0))     -- accepted
```

A name Oracle refuses bare — a reserved word such as `size`, `comment` or
`user`, or a name carrying a character outside a plain identifier — is quoted on
both sides instead, so the two still agree.

Two type mappings are worth knowing because Oracle has no direct equivalent:
`BOOLEAN` becomes `NUMBER(1)`, with `true` and `false` written as `1` and `0`,
and an enum becomes `VARCHAR2(255)` with a `CHECK` listing its values.

## What the 23 and 21 lines do differently

The `IF [NOT] EXISTS` guards are a 23-line feature. On 21 they are refused, and
the capability preset for that line reflects it, so a plan for a 21 server omits
them.

**Domains are a 23-line feature too**, and the only object where the two lines
differ rather than the spelling of a guard. Oracle 23 has a real `CREATE
DOMAIN`; Oracle 21 answers `ORA-00901` to it and has no `ALL_DOMAINS` view at
all. So a declared domain is planned, rendered, read back and compared on 23,
and on 21 it is refused before any SQL — a column declared with a type the
target cannot create would be left naming something the server has no
definition of.

Two things about Oracle's own catalog are worth knowing, because they decide
whether a domain converges. A domain declared `NOT NULL` grows a `CHECK` of its
own, named by the server and numbered per database, and Ptah reads the
nullability off the column instead so the plan does not carry that constraint
back and forth. And `DROP TABLE` is rendered with `PURGE`: a dropped table
keeps its dependencies in the recycle bin, and a plan that drops a table and
then the domain its column was typed by answers
`ORA-11538 ... has dependent objects in the recycle bin` halfway through. The
alternative, `DROP DOMAIN ... FORCE`, is worse — with a live dependent it
succeeds and silently untypes the column.

## Object types

**A composite type is Oracle's object type**, and the spelling is the whole
difference. PostgreSQL's `CREATE TYPE t AS (a NUMBER)` is *accepted* here and
creates nothing usable — `USER_TYPES` reports `ATTRIBUTES 0` with
`INCOMPLETE YES`, `USER_OBJECTS` reports `INVALID`, and the driver returns no
error at all. Ptah writes `CREATE OR REPLACE TYPE t AS OBJECT (...)`, which is
the statement that creates one, and reads it back from `ALL_TYPES` and
`ALL_TYPE_ATTRS`.

The read describes the subset the model can carry, and declines the rest by name
rather than flattening it: an object type with a method, a subtype, a collection
type (`VARRAY`, `TABLE OF`) and an incomplete shell are each left out, because
describing one by its attribute list alone would say a replay produces the same
type when it produces a different one. Replacing a type a column uses answers
`ORA-02303` and changes nothing — the server declining to leave that column
naming a shape it no longer has, which is kept rather than forced.

## Functions and procedures

**Functions and procedures** are rendered, read back and planned on both lines.
Their body is PL/SQL, which is what the declaration says:

```go
//ptah:schema:function name="fn_double" params="p IN NUMBER" returns="NUMBER" language="plsql" body="BEGIN RETURN p * 2; END;"
```

A declaration that omits `language=` is defaulted to `plpgsql`, and the renderer
names it and creates nothing rather than writing a body this server cannot run.
Two other shapes are named the same way: a parameter default, because
`ALL_ARGUMENTS` reports that one exists and never what it is, so a routine
created with one would be replanned on every run. And `volatility="STABLE"` is
refused, because Oracle reports determinism as `YES` or `NO` only — `IMMUTABLE`
is the `DETERMINISTIC` clause and `VOLATILE` is its absence, and there is no
third cell that does not either lie to a function-based index or diff forever.

The semicolon that closes a PL/SQL block belongs to the block rather than to the
client, which is why Oracle's own tooling ends one with a `/` on the next line.
A `CREATE` handed to the server without it returns **no error at all** and
leaves the object `INVALID`: `USER_TRIGGERS` still reports such a trigger
`ENABLED`, and `USER_PROCEDURES` omits the routine. Ptah keeps that
semicolon on every statement it sends.

## Open work

What remains for Oracle is tracked by
[#1875](https://github.com/stokaro/ptah/issues/1875) and
[#1920](https://github.com/stokaro/ptah/issues/1920).

## Next steps

- Which release lines are declared and at what support level: [Database support matrix](../support-matrix/).
- Capability keys per dialect: [Capabilities](../../reference/capabilities/).
- Declaring functions, types, and constraints in Go sources: [Go annotation reference](../../reference/go-annotations/).
