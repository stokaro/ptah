---
title: Go annotations
description: Use annotated Go structs as the desired database schema.
---

Use Go annotations when your Go application owns the schema and the database
should follow annotated model types. Ptah reads comments, not runtime Go tags,
so the model remains ordinary Go code.

## When to use them

| Use Go annotations when | Use another source when |
| --- | --- |
| The application structs already describe the domain. | A database team owns SQL or HCL directly. |
| You want code review to cover schema changes next to model changes. | You need an HCL schema construct Ptah has not implemented yet. |
| You want generated migrations from desired/live differences. | You only need to apply an existing migration directory. |

## Model the schema

The smallest annotation source that is still useful in a real project is a
table, a primary key, and a unique constraint:

```text
models/
  account.go
migrations/
```

Create `models/account.go`:

```go
package models

//ptah:schema:table name="accounts"
type Account struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int

	//ptah:schema:field name="email" type="TEXT" unique="true" not_null="true"
	Email string
}
```

Ptah recursively reads regular `*.go` files under each `--root-dir`. It skips
`*_test.go`, hidden directories, and directories named exactly `vendor`.
Names that merely contain that word, such as `myvendor`, remain part of the
source tree.

Render the desired SQL before connecting to a database:

```bash
ptah schema render --root-dir ./models --dialect postgres
```

Expected output includes:

```sql
CREATE TABLE "accounts" (
  "id" SERIAL PRIMARY KEY NOT NULL,
  "email" TEXT UNIQUE NOT NULL
);
```

The exact type rendering depends on the selected dialect and field tags. To
smoke-check without any daemon, render the SQLite dialect to a file:

```bash
ptah schema render --root-dir ./models --dialect sqlite >/tmp/ptah-schema.sql
sed -n '1,80p' /tmp/ptah-schema.sql
```

Standard output contains SQL only. Source-loading progress, schema counts, and
the dependency summary go to standard error, so the redirected file can be
executed unchanged. For PostgreSQL-family, MySQL-family, SQL Server, and
Spanner targets, Ptah creates all tables before adding foreign keys. SQLite
keeps foreign keys inline because it cannot add them after table creation.

Malformed foreign keys and constraints unsupported by the selected dialect
fail before Ptah emits any SQL. The output never silently omits a declared
foreign key. Ptah also checks referenced-key policy, compatible column types,
constraint-name scope, and dialect-specific index or storage restrictions.

Always pass `--dialect` when redirecting executable SQL. Without it, Ptah
attempts the built-in review targets and emits separate labeled sections only
if every target can render the schema. Any unsupported feature fails atomically
with empty standard output.

### Add an INCLUDE covering index

Use `include` to keep payload columns in a covering index without making them
search keys. Ptah preserves the comma-separated order after trimming whitespace:

```go
type AccountIndexes struct {
	//ptah:schema:index name="idx_accounts_email" fields="email" include="display_name,created_at" table="accounts"
	_ int
}
```

For PostgreSQL, YugabyteDB, and the Spanner PostgreSQL dialect, the annotation
renders `INCLUDE ("display_name", "created_at")`. PostgreSQL accepts the
default, `BTREE`, and `GIST` access methods, plus `SPGIST` on PostgreSQL 14 and
newer. YugabyteDB accepts the default and `LSM`; `BTREE` is its documented
alias for the default LSM and renders identically to the default. The Spanner
PostgreSQL dialect accepts only the default. CockroachDB and every other
dialect reject `include` before emitting SQL. Omit `include` when there are no
payload columns; a present list with an empty element is a parse error.

## Compare before changing data

For an existing database, inspect and compare first:

```bash
ptah db read --db-url "$DATABASE_URL"
ptah schema compare --root-dir ./models --db-url "$DATABASE_URL"
ptah migrations plan --root-dir ./models --db-url "$DATABASE_URL"
```

Review the plan output before generating files. Destructive changes should be
explicit and gated in CI.

## Generate and apply

```bash
ptah migrations generate \
  --root-dir ./models \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations

ptah migrations hash --dir ./migrations
ptah migrations validate --dir ./migrations
ptah migrations up --db-url "$DATABASE_URL" --migrations-dir ./migrations --verify-sum
```

For shared environments, add these guards:

```bash
ptah migrations validate --dir ./migrations
ptah migrations lint --dir ./migrations --dialect postgres
ptah migrations up \
  --db-url "$DATABASE_URL" \
  --migrations-dir ./migrations \
  --verify-sum \
  --dry-run
```

Run without `--dry-run` only after reviewing the generated SQL and committed
`ptah.sum`.

## Compose multiple sources

`--root-dir` is repeatable, so a desired schema can be assembled from several
Go packages, and the same commands mix Go roots with YAML, HCL, and SQL files
through repeatable `--schema-file`. The merge semantics — identity-based
deduplication, conflict detection, and per-root type ownership — are described
once on [Composite desired schema](../composite/).

## Verify across dialects

When a model change is surprising, or annotations are meant to be portable,
render more than one dialect:

```bash
ptah schema render --root-dir ./models --dialect postgres >/tmp/schema.pg.sql
ptah schema render --root-dir ./models --dialect mysql >/tmp/schema.mysql.sql
```

This catches annotations that are valid but map differently across dialects,
such as enum storage, serial columns, constraints, or generated columns.
Dialect differences are expected; the important check is that each target
renders valid SQL for the capabilities it supports.

## Move the schema to HCL

Start with a non-destructive export:

```bash
ptah schema export \
  --from go \
  --to hcl \
  --root-dir ./models \
  --out schema.hcl
```

Ptah parses the generated HCL and verifies that its canonical re-render is
stable before it writes `schema.hcl`. Every valid Go annotation semantic has an
HCL representation. Function, view, materialized-view, and trigger bodies are
emitted as opaque HCL strings, and Ptah reports a warning for each because it
does not structurally parse those dialect-specific SQL sub-languages. Review
every warning before treating the export as semantically complete. A separate
diagnostic reports any source string whose bytes change during Unicode NFC
normalization.

One export captures the complete selected Go source set and uses that immutable
view for both HCL parsing and cleanup planning. Ptah rechecks source membership,
file identity, permissions, and contents before publishing the HCL; a concurrent
source change aborts the export. The output directory is bound before staging,
and Ptah also rechecks an existing output's identity, permissions, and contents.
An output creation, edit, or replacement detected at this commit barrier is
left untouched and aborts publication. Successful HCL replacement is flushed
to durable storage before Go annotation cleanup starts.

Preview annotation removal only after the export has no diagnostics:

```bash
ptah schema export \
  --from go \
  --to hcl \
  --root-dir ./models \
  --out schema.hcl \
  --cleanup-go-annotations \
  --cleanup-diff
```

The diff mode writes the validated HCL file but does not modify Go source. Run
the same command without `--cleanup-diff` to apply the prevalidated cleanup
plan.

Before publishing HCL, cleanup accounts for every recognized standalone Ptah
directive in the captured Go AST. Each directive must use a placement listed in
the [Go annotation reference](../../reference/go-annotations/) and must produce
the corresponding parsed schema object. A misplaced role/function directive or
a file-scoped RLS directive that resolves to no RLS object stops the operation
with its source file and line; neither the HCL output nor Go sources change.
Comments that only share a prefix with a directive, such as
`//ptah:schema:tableau`, are ordinary comments and remain byte-for-byte intact.

:::caution[Cleanup is a one-time migration]
Cleanup fails before HCL publication when the export reports any diagnostic,
including the expected warning for each opaque SQL body. It also fails when the
output uses a `.go` path, aliases a protected source, a removable directive was
not represented in the parsed schema, or no removable annotations remain.

Export without cleanup, review the emitted bodies in
`schema.hcl`, then remove all Ptah schema annotations manually in one reviewed
change and switch the project to the HCL source. Do not rerun export after
manual removal starts. An export with no annotations fails and preserves the
existing HCL file. Annotations that produce no exportable HCL object fail with
the same preservation guarantee.

Ptah revalidates the Go source set before HCL publication and again before
cleanup. If a source changes between those steps, the HCL file remains published
but cleanup leaves every Go file unchanged and returns an error.

If a later source fails during a multi-file cleanup, Ptah rolls back earlier
replacements only while they still match the exact cleaned file it committed.
If that check detects a concurrent edit, Ptah leaves the edit in place,
preserves the original source in a `.ptah-backup-*` file, and reports its
location. These checks are optimistic; exclude uncooperative writers that can
mutate the same paths after the final commit barrier. Do not repeat cleanup
after a successful migration; use `schema.hcl` as the new source.
:::

## Next steps

- Looking up a directive or attribute? [Go annotation reference](../../reference/go-annotations/).
- Modeling in files instead of Go? [YAML schema](../yaml/), [HCL schema](../hcl/), or [SQL schema](../sql/).
- Embedding the parser in your own tool? [Public API](../../extend/public-api/).
