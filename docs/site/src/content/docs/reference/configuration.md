---
title: Configuration
description: Project config files, Atlas config subset, environment variables, and precedence.
---

Configuration precedence is:

| Rank | Source |
| --- | --- |
| 1 | Explicit CLI flags |
| 2 | Environment variables |
| 3 | `atlas.hcl` selected environment |
| 4 | `ptah.yaml` selected environment |
| 5 | Built-in defaults |

Use `ptah.yaml` for Ptah-owned configuration and the supported `atlas.hcl`
subset for Atlas-compatible project config. The supported Atlas subset includes
local `variable` defaults and Atlas-style `--var name=value` overrides,
`locals`, `getenv`, `file`, `fileset`, and `data.hcl_schema.<name>.url`
references for local schema-file workflows.
Supported Atlas env blocks can also set `schema.src`, `schema.mode`, `format`,
and local `diff` policy defaults for `ptah atlas ...` commands.

Ptah reads each selected project config once per command and converts it to a
typed configuration value. Migration database settings and online-DDL policy
therefore cannot come from different generations of a concurrently replaced
`ptah.yaml`. An explicit `--config` path must exist; the conventional
`./ptah.yaml` is optional.

For Atlas-compatible commands, plain local schema paths, relative `file://`
schema URLs, and relative `migration.dir` values declared in `atlas.hcl` resolve
relative to the directory containing that `atlas.hcl` file. Explicit CLI path
flags such as `--to`, `--from`, and `--dir` keep CLI semantics and resolve
relative to the process working directory unless they are absolute.

## Minimal `ptah.yaml`

```yaml
env:
  dev:
    url: sqlite:////tmp/ptah-dev.db
    migration:
      dir: ./migrations
```

Run with the named environment:

```bash
ptah migrations status --env dev
ptah migrations up --env dev --verify-sum
```

If a config file has multiple environments, pass `--env`. Ptah fails instead of
guessing.

## Operational settings

Project config can also define timeouts, revision table layout, migration
directory format, transaction mode, backup destinations, pre-flight hooks,
webhooks, lint defaults, and online-DDL policy.

| Setting area | Example keys |
| --- | --- |
| Database target | `url`, `src`, `schema.src`, `dev`, `schemas` |
| External desired schema | `external_schema.program`, `external_schema.format`, `external_schema.working_dir`, `external_schema.env` |
| Migration directory and revisions | `migration.dir`, `migration.format`, `migration.revisions_table`, `migration.revision_format` |
| Safety and operations | `migration.pre_up_hook`, `migration.pg_dump_to`, `migration.webhook`, `migration.exec_order`, `migration.tx_mode` |
| Lint defaults and policy | `lint.dialect`, `lint.disabled-rules`, `lint.latest`, `lint.git.base`, `lint.destructive.error`, `lint.concurrent_index.error` |
| Online DDL | `online_ddl.tool`, `online_ddl.threshold_rows`, `online_ddl.args`, `online_ddl.fallback` |
| Diff policy (native `migrations generate`) | `diff.skip: [drop_table, drop_column, drop_index, drop_enum]`, `diff.concurrent_index` |
| Atlas-compatible output | `format.schema.inspect`, `format.schema.apply`, `format.schema.clean`, `format.schema.diff`, `format.migrate.apply`, `format.migrate.diff`, `format.migrate.lint`, `format.migrate.status` |
| Atlas-compatible diff policy | `diff.skip.drop_table`, `diff.concurrent_index.create` |

The native `diff` block shapes what `ptah migrations generate` emits: `diff.skip`
lists destructive change kinds (`drop_table`, `drop_column`, `drop_index`,
`drop_enum`) to omit — a `-- SKIP: ...` comment is written in their place — and
`diff.concurrent_index: true` requests `CREATE INDEX CONCURRENTLY` for new
indexes (PostgreSQL, capability-gated). A skipped change is never emitted, so it
never trips the `--check-destructive` gate.

The Atlas-compatible command tree lives under `ptah atlas <command> ...`.
Atlas project flags such as `--config`, `-c`, `--env`, and repeated
`--var name=value` belong to this tree only.
`ptah-compat` is the drop-in replacement binary for scripts that expect
Atlas-style root commands; it is not a separate configuration surface.

## External desired schema

Use `external_schema` when an ORM, framework, or generator owns the desired
schema:

```yaml
external_schema:
  program: [".venv/bin/atlas-provider-sqlalchemy", "--path", "./models", "--dialect", "postgresql"]
  format: sql
  working_dir: ./app
  env: ["APP_ENV=dev"]
```

`program` is an explicit argument list and is executed without a shell.
`format` accepts `sql` (the default), `hcl`, or `yaml`. The block supplies the
desired schema to native `schema render`, `schema compare`, `schema drift`,
`migrations plan`, and `migrations generate` commands when `--schema-cmd` is
not set. An explicit command and its `--schema-format` take precedence;
`--schema-cmd=` disables the configured source. `--env` selects an
environment-scoped block. Empty command output is rejected. `PATH` and `PWD`
cannot be overridden through `external_schema.env`; use an explicit executable
path and `working_dir`.

Auto-discovered configuration never executes a program implicitly. Pass
`--allow-external-schema` to use the configured block. An explicit
`--schema-cmd` is already an opt-in and does not require that additional flag.
Relative `working_dir` values are constrained to the current working directory
after symlink resolution. Ptah bounds output, redacts secrets and terminal
control characters from stderr and parser diagnostics, and cleans up descendant
processes.

Continue with [Atlas project config](../../atlas/project-config/) for the supported
`atlas.hcl` subset.

:::note
Ptah config parsing is intentionally strict. Unknown `ptah.yaml` keys and unsupported `atlas.hcl` constructs fail instead of being ignored.
:::
