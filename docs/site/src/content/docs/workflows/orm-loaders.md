---
title: ORM loaders
description: Feed an ORM's schema into Ptah with the external_schema source.
---

Ptah can take its desired schema from an ORM instead of Go annotations or a
static schema file. The [external-command source](../schema-files/#load-from-an-external-program)
runs a program you choose and reads its standard output as the desired schema —
so any tool that prints a schema as SQL, HCL, or YAML becomes a Ptah source.

This is Ptah's open, local, MIT equivalent of Atlas's `data "external_schema"`
and its ORM provider loaders. Provider compatibility depends on the emitted
schema syntax; the recipes below are verified end to end with Ptah.

## The contract

An ORM loader is any program that prints the **complete desired schema** as SQL,
HCL, or YAML to stdout. Ptah runs it directly without a shell, parses the
selected format, and uses the result wherever a desired schema is needed:

```bash
ptah schema render   --schema-cmd "<loader>" --dialect postgres
ptah schema compare  --schema-cmd "<loader>" --db-url "$DATABASE_URL"
ptah schema drift    --schema-cmd "<loader>" --db-url "$DATABASE_URL"
ptah migrations plan --schema-cmd "<loader>" --db-url "$DATABASE_URL"
```

Primary keys, foreign keys, unique constraints, and indexes in the emitted DDL
are all captured, so the loaded schema diffs and migrates faithfully.

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
SQLAlchemy, Sequelize, TypeORM, Hibernate, and more). Each one prints SQL DDL to
stdout, so compatible output can plug into `--schema-cmd` / `external_schema`
like the verified recipes above. Any tool that can emit a complete SQL, HCL, or
YAML schema can work too, including a hand-written script that runs a
framework's schema dumper.

The Ptah side is always identical — you just supply the loader command:

```yaml
external_schema:
  program: ["<the loader command and its arguments>"]
```

Consult the provider's documentation for the exact loader command (the Atlas
provider packages are named `atlas-provider-<orm>`), then run it once by hand to
confirm it prints a complete supported schema before wiring it into Ptah. GORM
and SQLAlchemy are verified against Ptah here; treat other providers as starting
points and check their emitted schemas parse cleanly.

## Notes

- **Format**: the external command's output is parsed as SQL by default.
  `--schema-format hcl` and `--schema-format yaml` select the other supported
  formats; `yml` is accepted as a YAML alias.
- **No shell**: the program is run with an explicit argument vector, so shell
  features (pipes, globbing, variable expansion) are not available. Wrap a
  complex invocation in a small script and point the loader at that.
- **Explicit trust**: `external_schema` loaded from `ptah.yaml` requires
  `--allow-external-schema`; an explicit `--schema-cmd` does not.
- **Environment**: `external_schema.env` cannot override `PATH` or `PWD`. Use
  an explicit executable path and `working_dir`.
- **Lifecycle and errors**: execution is bounded by a timeout, descendant
  processes are cleaned up, and empty output is rejected. Failures include a
  bounded, secret-redacted, terminal-safe stderr tail or parser diagnostic.
