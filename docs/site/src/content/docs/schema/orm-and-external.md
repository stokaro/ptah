---
title: ORM and external loaders
description: Feed an ORM's or any external program's schema into Ptah as the desired schema.
---

When the desired schema lives in an ORM or framework rather than in Go
annotations or a static file, Ptah runs an external program and reads its
standard output as the desired schema. Any tool that prints a schema as SQL,
HCL, or YAML becomes a Ptah source — including the Atlas ecosystem's ORM
provider loaders.

This is Ptah's open, local, MIT equivalent of Atlas's `data "external_schema"`
source and its ORM provider loaders. Provider compatibility depends on the
emitted schema syntax; the recipes below are verified end to end with Ptah.

## The contract

An ORM loader is any program that prints the **complete desired schema** as
SQL, HCL, or YAML to stdout. Ptah runs it directly — never through a shell —
parses the selected format, and uses the result wherever a desired schema is
needed:

```bash
ptah schema render   --schema-cmd "<loader>" --dialect postgres
ptah schema compare  --schema-cmd "<loader>" --db-url "$DATABASE_URL"
ptah schema drift    --schema-cmd "<loader>" --db-url "$DATABASE_URL"
ptah migrations plan --schema-cmd "<loader>" --db-url "$DATABASE_URL"
```

Primary keys, foreign keys, unique constraints, and indexes in the emitted DDL
are all captured, so the loaded schema diffs and migrates faithfully.

## Run a loader with `--schema-cmd`

`--schema-cmd` takes the program as a single string:

```bash
ptah schema render --schema-cmd "./scripts/export-schema" --dialect postgres
```

- **Format**: stdout is parsed as SQL by default. Set `--schema-format hcl` or
  `--schema-format yaml` when the loader emits another supported source
  format; `yml` is accepted as a YAML alias.
- **No shell**: the program runs with an explicit argument vector split on
  whitespace, so pipes, globbing, and variable expansion are not available and
  arguments cannot contain spaces. Wrap a more complex invocation in a small
  script and point `--schema-cmd` at that.
- **Lifecycle**: execution is bounded by a timeout, and Ptah owns the loader's
  process tree — a process group on Unix, a kill-on-close Job Object on
  Windows — so descendants are cleaned up on cancellation, timeout, and after
  a successful parent exit.
- **Errors**: if the program exits non-zero, its stderr is surfaced in the
  error as a bounded, secret-redacted, terminal-safe tail. Stdout is bounded,
  and empty or whitespace-only output is rejected so a broken provider cannot
  be interpreted as an intentionally empty desired schema.

## Configure it in `ptah.yaml`

Instead of the flag, declare the loader once in an `external_schema` block.
Unlike `--schema-cmd` — a single string split on whitespace — the config form
takes an explicit argument list, so arguments may contain spaces:

```yaml
external_schema:
  program: ["go", "run", "ariga.io/atlas-provider-gorm", "load", "--path", "./models"]
  format: sql          # optional, defaults to sql
  working_dir: ./app   # optional; defaults to the current directory
  env: ["APP_ENV=dev"] # optional extra KEY=VALUE entries
```

`ptah schema render`, `ptah schema compare`, `ptah schema drift`,
`ptah migrations plan`, and `ptah migrations generate` read this block when
`--schema-cmd` is not passed (the flag always wins). Auto-discovered config is
not permission to execute repository-controlled code, so a config-sourced
loader also requires `--allow-external-schema`. Those commands read the other
settings they consume, such as `url` and `schemas`, and honor `--env` to
select an env block:

```bash
ptah schema drift \
  --config ptah.yaml \
  --allow-external-schema
```

Set `--schema-cmd=` explicitly to disable the configured source for one
invocation.

Constraints on the block:

- `external_schema.env` cannot override `PATH` or `PWD`. Put the executable
  path in `program` explicitly and use `working_dir` to select the command
  directory.
- Relative `working_dir` values must remain inside the process working
  directory after symlink resolution. Use an explicit absolute path for a
  deliberately external loader.
- The block mirrors the desired-state role of Atlas's
  `data "external_schema"` but does not evaluate that Atlas HCL data source.

## GORM (verified)

The committed [`examples/orm-loaders/gorm`](https://github.com/stokaro/ptah/tree/master/examples/orm-loaders/gorm)
recipe contains a small GORM model module. It invokes the official standalone
[`atlas-provider-gorm`](https://github.com/ariga/atlas-provider-gorm) command
through `load-schema.sh` at the pinned version `v0.6.1`. The script uses an
ephemeral copy of the model module, so the provider and its transitive
dependencies do not become part of the application module's dependency graph.

Download the model dependency and render the schema through the checked-in
`ptah.yaml`:

```bash
cd examples/orm-loaders/gorm
go mod download
ptah schema render \
  --config ptah.yaml \
  --allow-external-schema \
  --dialect postgres
```

The generated schema includes the `users` and `pets` tables with their primary
keys, the unique index on `email`, and the `pets → users` foreign key.

## SQLAlchemy (verified)

The committed [`examples/orm-loaders/sqlalchemy`](https://github.com/stokaro/ptah/tree/master/examples/orm-loaders/sqlalchemy)
recipe pins both the SQLAlchemy provider and SQLAlchemy itself. Its models
declare `users` and `pets`, a unique user email, and a cascading
`pets.user_id → users.id` foreign key.

Create the isolated Python environment and render it through the checked-in
`ptah.yaml`:

```bash
cd examples/orm-loaders/sqlalchemy
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.lock.txt
ptah schema render \
  --config ptah.yaml \
  --allow-external-schema \
  --dialect postgres
```

The lock file records the complete environment verified with Python 3.12,
`atlas-provider-sqlalchemy==0.4.1`, and `SQLAlchemy==2.0.50`. The provider
emitted PostgreSQL DDL, and Ptah parsed and rendered both tables, both primary
keys, the unique email constraint, and the foreign key with
`ON DELETE CASCADE`.

## Other ORMs

The Atlas ecosystem publishes schema loaders for many ORMs (GORM, Django,
SQLAlchemy, Sequelize, TypeORM, Hibernate, and more). Each one prints SQL DDL
to stdout, so compatible output can plug into `--schema-cmd` /
`external_schema` like the verified recipes above. Any tool that can emit a
complete SQL, HCL, or YAML schema can work too, including a hand-written
script that runs a framework's schema dumper.

The Ptah side is always identical — you supply the loader command:

```yaml
external_schema:
  program: ["<the loader command and its arguments>"]
```

Consult the provider's documentation for the exact loader command (the Atlas
provider packages are named `atlas-provider-<orm>`), then run it once by hand
to confirm it prints a complete supported schema before wiring it into Ptah.
GORM and SQLAlchemy are verified against Ptah here; treat other providers as
starting points and check their emitted schemas parse cleanly.

## Compose with other sources

An external command composes with Go roots and schema files, so you can merge
an ORM export with a vendored HCL file:

```bash
ptah schema render \
  --schema-cmd "./scripts/export-schema" \
  --schema-file ./vendor/thirdparty.hcl \
  --dialect postgres
```

The merge and conflict rules are on [Composite desired schema](../composite/).

## Next steps

- Combining the loader with more sources? [Composite desired schema](../composite/).
- Drift-checking a live database against the ORM? [Migrations](../../workflows/migrations/) covers plan and apply.
- Declaring the loader per environment? [Configuration](../../reference/configuration/).
