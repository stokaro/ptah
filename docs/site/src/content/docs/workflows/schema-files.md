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

Sources are merged and finalized together; identical objects are deduplicated and
conflicting definitions are an error. This is Ptah's open, local, no-account
equivalent of Atlas's Pro-only `composite_schema` data source. For composing
multiple Go packages — including on `ptah schema compare` and the migration
commands — see [Go schema](../go-schema/).

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

The command's stdout is parsed as SQL by default; set `--schema-format sql`
explicitly if you prefer. Execution is bounded by a timeout, and if the program
exits non-zero its stderr is surfaced in the error. Because the program is run
with an explicit argument vector split on whitespace, arguments cannot contain
spaces — wrap a more complex invocation in a small script and point
`--schema-cmd` at that.

An external command composes with the other sources, so you can, for example,
merge an ORM export with a vendored HCL file:

```bash
ptah schema render \
  --schema-cmd "./scripts/export-schema" \
  --schema-file ./vendor/thirdparty.hcl \
  --dialect postgres
```

This is Ptah's open, local, MIT equivalent of Atlas's `data "external_schema"`
source and its ORM provider loaders.

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

`ptah schema compare`, `ptah schema drift`, `ptah migrations plan`, and
`ptah migrations generate` read this block when `--schema-cmd` is not passed (the
flag always wins). Those commands also read `url` (the database) and `schemas`
from `ptah.yaml`, and honor `--env` to select an env block — so a drift check can
be as short as `ptah schema drift --config ptah.yaml`. This mirrors Atlas's
`data "external_schema"` block: the program must print the complete desired
schema — currently SQL DDL — to stdout.
