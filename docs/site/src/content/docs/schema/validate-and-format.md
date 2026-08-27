---
title: Validate and format schema files
description: Check a desired schema for structural problems with ptah schema validate and keep HCL files canonically formatted with ptah schema fmt, both without a database.
---

Two native verbs check schema files before anything connects to a database.
`ptah schema validate` reports structural problems in a desired schema, once per
target dialect. `ptah schema fmt` rewrites HCL schema files into HashiCorp HCL's
canonical layout, or reports the ones that are not in it.

Use them as a pre-commit hook and as the first job of a pipeline. Neither one
takes a `--db-url`, so both run on a machine with no database and finish in the
time a linter takes.

| Command | Reads | Answers |
| --- | --- | --- |
| `ptah schema validate` | any desired-schema source | Is this schema structurally sound for the dialects I target? |
| `ptah schema fmt` | `.hcl` files on disk | Are these files in canonical HCL layout? |

Prerequisites: an installed `ptah` binary ([Install Ptah](../../start/install/))
and a desired schema as local files.

## Starting state

Save this as `broken.yaml`. It declares one table with two problems: an index
over a column the table does not have, and a foreign key to a table nothing
declares.

```yaml
tables:
  orders:
    columns:
      id:
        type: SERIAL
        primary: true
      customer_id:
        type: INTEGER
        not_null: true
        foreign: customers(id)
    indexes:
      idx_orders_status:
        fields: [status]
```

Save this beside it as `schema.hcl`. It declares a sound schema in a layout
that is not canonical:

```hcl
schema "public" {}
table "customers" {
schema = schema.public
column "id" {
type = int
}
column   "email"   {
   type = varchar(255)
     null = false
}
}
```

## Validate against one dialect

`--dialect` is required, because a declaration valid for one target can be
invalid for another:

```bash
ptah schema validate --schema-file broken.yaml --dialect postgres
```

Expected output includes:

```text
postgres: index "idx_orders_status": names column "status", which table "orders" does not declare
postgres: schema: invalid foreign key: field "customer_id" references unknown table "customers"
2 structural problems
```

The run exits `1`. Every line names the dialect it was found under, then the
object, then what is wrong with it. A schema with nothing wrong prints nothing
and exits `0`, so a hook can read the status alone.

## Validate against every dialect you ship to

`--dialect` is repeatable, and each value is checked separately with its own
lines:

```bash
ptah schema validate --schema-file broken.yaml --dialect postgres --dialect mysql
```

Expected output includes:

```text
postgres: index "idx_orders_status": names column "status", which table "orders" does not declare
postgres: schema: invalid foreign key: field "customer_id" references unknown table "customers"
mysql: index "idx_orders_status": names column "status", which table "orders" does not declare
mysql: schema: invalid foreign key: field "customer_id" references unknown table "customers"
4 structural problems
```

A problem that only one target has is reported under that target alone. A
schema declaring a foreign key validates on `postgres` and fails on
`clickhouse` with `clickhouse: schema: clickhouse does not support foreign
keys`, because ClickHouse models none.

`--root-dir` reads [Go annotations](../go-annotations/) instead of a file, and
`--schema-file` is repeatable. Naming both merges them into one
[composite desired schema](../composite/). `--server-version` refines the
capability set a dialect stands for, for example `--dialect postgres
--server-version 17`.

## Format HCL schema files

`ptah schema fmt` walks the paths it is given, or the current directory when it
is given none, and rewrites every `.hcl` file whose layout is not canonical. It
prints the files it changed:

```bash
ptah schema fmt .
```

Expected output includes:

```text
schema.hcl
```

Only files whose content changed are printed, so a run that changes nothing
prints nothing. The rewrite is HashiCorp HCL's own canonical layout —
indentation, alignment and spacing — and it changes no value the file declares.
`schema.hcl` becomes:

```hcl
schema "public" {}
table "customers" {
  schema = schema.public
  column "id" {
    type = int
  }
  column "email" {
    type = varchar(255)
    null = false
  }
}
```

## Gate a pipeline on formatting

`--check` rewrites nothing. It prints the files that are not canonically
formatted and refuses:

```bash
ptah schema fmt --check .
```

Expected output includes:

```text
schema.hcl
error: 1 file(s) are not canonically formatted; run `ptah schema fmt` to rewrite them
```

That run exits `2`, not `1`: an unformatted file is reported as a command
failure rather than as an expected negative result. A pipeline step that treats
any non-zero status as a failure needs no special handling; one that
distinguishes `1` from `2` has to know this.

## Failure modes

| Message on stderr | Cause | Exit |
| --- | --- | --- |
| `error: --dialect is required: validation is per target, and a declaration valid for one dialect can be invalid for another` | `schema validate` with no `--dialect`. | `2` |
| `error: schema fmt nosuch.hcl: stat nosuch.hcl: no such file or directory` | `schema fmt` given a path that does not exist. | `2` |
| `error: 1 file(s) are not canonically formatted; run \`ptah schema fmt\` to rewrite them` | `schema fmt --check` found files to rewrite. | `2` |

See [Exit codes](../../reference/exit-codes/) for the contract these follow.

## Limitations

- `ptah schema validate` checks structure, not renderability. A declaration the
  renderer refuses for the same dialect can validate cleanly: a `SERIAL` column
  validates against `clickhouse` and exits `0`, while
  `ptah schema render --dialect clickhouse` over the same source exits `2` with
  `clickhouse: SERIAL has no auto-increment equivalent`. Render as well as
  validate before trusting a target.
- `ptah schema fmt` reads `.hcl` files only. A YAML, SQL or DBML schema file in
  the same directory is left alone and not reported, so a formatting gate over a
  mixed directory covers the HCL half of it.
- `ptah schema fmt` takes no `--config` and no `--env`. It works on paths, not
  on the schema sources a project configuration names.

## Exact reference

Run `ptah schema validate --help` and `ptah schema fmt --help` for the flag sets
with their environment variables.
[Native commands](../../reference/native-commands/) places both verbs in the
tree, and [Exit codes](../../reference/exit-codes/) carries a row for each.

## Next steps

- Ready to see the SQL the schema renders to?
  [Work with a schema source](../work-with-a-source/).
- Want the same check against a live database?
  [Compare and drift](../../direct/compare-and-drift/).
- Adding these to a pipeline? [Continuous integration](../../testing/ci/).
