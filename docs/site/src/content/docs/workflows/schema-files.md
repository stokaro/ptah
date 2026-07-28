---
title: Schema files
description: Use YAML, HCL, or SQL schema files as Ptah input.
---

Ptah can render and migrate from schema files when Go annotations are not the
source of truth.

## Pick a source format

| Format | Best for | Notes |
| --- | --- | --- |
| Ptah YAML | Ptah-owned schema files with compact structure. | Strict parser; unknown keys fail. |
| HCL schema | Reusing supported Atlas-compatible schema files. | Supported subset only; unsupported constructs fail. |
| SQL schema | Reusing local SQL DDL files for render and Atlas-compatible local diff workflows. | Parsed through Ptah's compatibility SQL parser; unsupported DDL fails explicitly. |
| Live database | Introspection, drift checks, and migration planning. | Requires a database URL. |

## YAML schema

YAML is Ptah-owned and strict. Use it when you want a compact, explicit schema file without HCL syntax:

```yaml
tables:
  users:
    columns:
      id:
        type: SERIAL
        primary: true
      email:
        type: VARCHAR(255)
        not_null: true
```

```bash
ptah schema render --schema-file schema.yaml --dialect postgres
```

Use the same input to plan against a live database:

```bash
ptah migrations plan \
  --schema-file schema.yaml \
  --db-url "$DATABASE_URL"
```

Reference: [YAML schema](../../reference/yaml-schema/).

## HCL schema

Use HCL schema files when you already maintain schema files in
Atlas-compatible HCL syntax and want Ptah to read the supported subset:

```hcl
schema "public" {}

table "users" {
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

```bash
ptah schema render --schema-file schema.hcl --dialect postgres
```

Ptah reads schema HCL as desired schema input. Project configuration HCL is a
different file type and is described in [Configuration](../../reference/configuration/).

Reference: [HCL schema](../../reference/hcl-schema/).

:::caution[Supported subset]
Ptah's HCL schema format is compatible with the Atlas HCL schema language for
the supported subset. Ptah is not affiliated with or endorsed by Ariga or Atlas.
Unsupported constructs fail explicitly instead of being silently guessed. If a
construct is not implemented, treat the error as a compatibility gap and check
the conformance reports.
:::

## SQL schema

Use SQL schema files when the desired state is already represented as local DDL:

```sql
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL
);
```

```bash
ptah schema render --schema-file schema.sql --dialect sqlite
```

The same local SQL files can be compared through the Atlas-compatible command
surface:

```bash
ptah atlas schema diff \
  --from file://old.sql \
  --to file://schema.sql \
  --dev-url "sqlite://dev?mode=memory"
```

## Validate before applying

Preview the SQL Ptah derives from your schema file and review it before you apply anything:

```bash
ptah schema render --schema-file schema.yaml --dialect postgres >/tmp/schema.sql
```

The rendered SQL proves Ptah understood the desired schema. `--schema-file` is
accepted wherever Ptah needs a desired schema: `ptah schema render`,
`ptah schema compare`, `ptah schema drift`, and the migration commands
(`ptah migrations plan` / `ptah migrations generate`). Each command reads the
desired schema from Go entities (`--root-dir`), schema files (`--schema-file`),
or any combination of the two.

## Compose multiple sources

`--schema-file` is repeatable, and `ptah schema render` can combine several
schema files — and Go roots — into one desired schema. Mix formats freely: a Go
package for app tables, a vendored HCL file for third-party tables, and a YAML
file for shared lookups all merge into a single render:

```bash
ptah schema render \
  --root-dir ./models \
  --schema-file ./vendor/thirdparty.hcl \
  --schema-file ./shared/lookups.yaml \
  --dialect postgres
```

Sources are merged and finalized together. At source boundaries, Ptah checks
every named object by its database identity: schema-qualified names where the
object supports schemas, table-qualified names for columns, indexes,
constraints, triggers, and RLS policies, and global names for extensions,
functions, enums, and roles. Identical definitions are deduplicated even when
their parser-only Go names differ. If the same identity has different desired
properties, Ptah stops before rendering or connecting to a database with an
error such as:

```text
error merging composite schema: conflicting field "id" definitions on table "users"
```

Treat each repeatable `--root-dir` and `--schema-file` value, plus the selected
`--schema-cmd` when present, as one ownership boundary. Ptah applies the same
strict database-identity conflict checks inside a Go root and across source
boundaries. Separate roots also preserve source-root-local helper type ownership,
so identical helper names in different roots cannot mix columns between
schema-qualified tables. The isolation includes direct and nested embedded
helper types. A single recursively scanned root remains one type namespace.
Managed-data file references retain their declaring Go root rather than being
reinterpreted relative to whichever root happened to be loaded first.

This is Ptah's open, local, no-account equivalent of Atlas's Pro-only
`composite_schema` data source. For composing multiple Go packages — including
on `ptah schema compare` and the migration commands — see
[Go schema](../go-schema/).

## Load from an external program

When the desired schema lives in an ORM or framework rather than in Go
annotations or a static file, `--schema-cmd` runs an external program and reads
its standard output as the desired schema. The program is executed directly —
never through a shell — so an ORM's own schema exporter can feed Ptah's engine:

```bash
ptah schema render --schema-cmd "./scripts/export-schema" --dialect postgres
```

`--schema-cmd` is accepted wherever Ptah needs a desired schema —
`ptah schema render`, `ptah schema compare`, `ptah schema drift`, and the
migration commands (`ptah migrations plan` / `ptah migrations generate`) — so you
can diff, plan, migrate, and drift-check a live database against an ORM's schema:

```bash
ptah schema drift --schema-cmd "./scripts/export-schema" --db-url "$DATABASE_URL"
```

The command's stdout is parsed as SQL by default. Set `--schema-format hcl` or
`--schema-format yaml` when the loader emits another supported source format;
`yml` is accepted as a YAML alias. Execution is bounded by a timeout, and if the
program exits non-zero its stderr is surfaced in the error. Because the program
is run with an explicit argument vector split on whitespace, arguments cannot
contain spaces — wrap a more complex invocation in a small script and point
`--schema-cmd` at that.

Ptah owns the loader's process tree: it uses a process group on Unix and a
kill-on-close Job Object on Windows. Descendants are cleaned up on cancellation,
timeout, and after a successful parent exit. Stdout is bounded, and failures
include only bounded, secret-redacted, terminal-safe diagnostics from stderr or
output parsing. Empty or whitespace-only stdout is rejected so a broken
provider cannot be interpreted as an intentionally empty desired schema.

An external command composes with the other sources, so you can, for example,
merge an ORM export with a vendored HCL file:

```bash
ptah schema render \
  --schema-cmd "./scripts/export-schema" \
  --schema-file ./vendor/thirdparty.hcl \
  --dialect postgres
```

This is Ptah's open, local, MIT equivalent of Atlas's `data "external_schema"`
source and its ORM provider loaders. For ready-made loaders — including a
verified GORM recipe and a separately verified SQLAlchemy recipe — see
[ORM loaders](../orm-loaders/).

### Configure it in `ptah.yaml`

Instead of the flag, declare the loader once in an `external_schema` block. Unlike
`--schema-cmd` — which is a single string split on whitespace — the config form
takes an explicit argument list, so arguments may contain spaces:

```yaml
external_schema:
  program: ["go", "run", "ariga.io/atlas-provider-gorm", "load", "--path", "./models"]
  format: sql          # optional, defaults to sql
  working_dir: ./app   # optional; defaults to the current directory
  env: ["APP_ENV=dev"] # optional extra KEY=VALUE entries
```

`external_schema.env` cannot override `PATH` or `PWD`. Put the executable path
in `program` explicitly and use `working_dir` to select the command directory.

`ptah schema render`, `ptah schema compare`, `ptah schema drift`,
`ptah migrations plan`, and `ptah migrations generate` read this block when
`--schema-cmd` is not passed (the flag always wins). Auto-discovered config is
not permission to execute repository-controlled code, so a config-sourced
loader also requires `--allow-external-schema`. Those commands read the other
settings they consume, such as `url` and `schemas`, and honor `--env` to select
an env block:

```bash
ptah schema drift \
  --config ptah.yaml \
  --allow-external-schema
```

Set `--schema-cmd=` explicitly to disable the configured source for one
invocation.

Relative `working_dir` values must remain inside the process working directory
after symlink resolution. Use an explicit absolute path for a deliberately
external loader. This native block mirrors the desired-state role of Atlas's
`data "external_schema"` but does not evaluate that Atlas HCL data source.
