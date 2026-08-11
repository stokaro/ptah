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

A `PTAH_<FLAG>` value must parse as the corresponding flag type. Ptah rejects a
malformed value before argument validation, command hooks, or database work
begins. For example, `PTAH_DRY_RUN=notabool` fails with
`invalid boolean value "notabool" for PTAH_DRY_RUN` instead of running with
the default `false` value. An explicit CLI flag wins without reading its
environment twin, including when that environment value is malformed. An empty
value remains unset for every flag type except boolean, where it is rejected;
see [Boolean environment variables](#boolean-environment-variables).

## Boolean environment variables

Every boolean `PTAH_*` variable follows one rule, whether it is a flag's
environment twin or a feature toggle read directly:

| State | Result |
| --- | --- |
| Not set | The documented default |
| Set to a valid boolean | Parsed and honored |
| Set to anything else | The command fails before doing any work |

The accepted spellings are Go's `strconv.ParseBool` spellings:

```text
true:  1  t  T  true  True  TRUE
false: 0  f  F  false  False  FALSE
```

Nothing is trimmed before parsing. `" true"`, `"true "` and an exported empty
value are all rejected, because a quoting or expansion mistake in a YAML
manifest, a CI environment file or a systemd unit is exactly the configuration
error this rule exists to surface. The refusal names the variable and the raw
value:

```text
Error: invalid boolean value "maybe" for PTAH_POSTGRES_INSPECT_ALL_ROLES
```

The refusal comes before the command connects to a database, writes a migration
file, applies schema changes, touches revision state, or produces a
machine-readable result on standard output. It also comes before the command's
own early returns: an invalid value is rejected even on a run that would never
have consulted it, such as a `migrate lint` that already names `--latest`, or a
`schema apply` whose `--exclude` selectors all match.

A command only validates the variables it owns. A malformed PostgreSQL
inspection variable does not break an unrelated SQLite command or
`ptah version`.

Non-boolean `PTAH_*` variables are unchanged: an empty value still reads as
unset for them.

### Atlas CE strict compatibility

`PTAH_ATLAS_STRICT_COMPAT` is a `ptah-compat`-only boolean policy selector. It
defaults to `false`, which keeps the complete Pro-like and best-effort
compatibility surface. Set it to `true` only for Atlas Community Edition oracle
or conformance runs:

```bash
PTAH_ATLAS_STRICT_COMPAT=1 ptah-compat migrate validate --dir file://migrations
```

The process resolves the selector before command construction. In strict mode,
generic `PTAH_<FLAG>` environment twins are not installed because Atlas CE does
not expose them. A present extension variable, including one whose command flag
is absent from the strict tree, is rejected rather than ignored. False values
for known boolean extension toggles remain valid; malformed values retain the
normal boolean diagnostic.

The check is based on Ptah's actual flag bindings and feature-toggle registry,
not on the `PTAH_` prefix alone. An ordinary value that an `atlas.hcl` reads
through `getenv`, such as `PTAH_DATABASE_PASSWORD`, remains available in strict
mode. Naming project inputs with that prefix does not turn them into Ptah
features.

Strict mode also applies its object inventory to live schemas. `schema inspect`,
`schema apply`, and `schema clean` refuse a Pro-only object before rendering or
mutation instead of copying CE behavior that could omit or destroy it. On
`schema clean`, the CE-registered `--dry-run` and `--format` flags retain the
pinned binary's community-abort behavior. Leave strict mode off for Ptah's
complete cleanup, inspection, and apply capabilities.

The cleanup check covers the writer's complete destruction inventory. That
includes PostgreSQL procedures, aggregates, foreign tables, collations,
default privileges, and dependent objects such as triggers that disappear
with a table without appearing as a separate cleanup plan line.

Strict schema workflows also refuse YAML sources and an authored `schema apply`
lint policy that the CE execution path cannot enforce. Commands that execute,
convert, or replay migration bodies refuse Atlas txtar, every Ptah directive,
and SQL templates; checksum-only reads preserve those bytes. All inputs remain
available in the default, complete compatibility profile.

Four opt-in correctness controls remain available because they do not add an
Atlas capability: `PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE`,
`PTAH_HCL_STRICT_REDECLARATIONS`, `PTAH_STRICT_DIR_QUERY`, and
`PTAH_ALLOW_NONINTERACTIVE_EDIT`. The last one permits a scripted editor in a
non-interactive process; it does not add an editor or migration capability.
Native `ptah` does not read `PTAH_ATLAS_STRICT_COMPAT`.

Project-file merging preserves source presence. For a supported field, an
explicitly present value replaces the lower-precedence value instead of being
treated as absent. This includes an empty string, zero, `false`, or an empty
list when the field accepts that type. Thus `atlas.hcl` wins over `ptah.yaml`,
while environment variables and explicit CLI flags still win.
After project sources are merged, a command applies its built-in default only
when a field is absent. An explicitly present empty or zero value instead
reaches normal validation. Fields that do not accept empty values, including
Atlas format templates, fail during parsing or command validation.

Use `ptah.yaml` for Ptah-owned configuration and the supported `atlas.hcl`
subset for Atlas-compatible project config. The supported Atlas subset includes
local `variable` defaults and Atlas-style `--var name=value` overrides,
`locals`, `getenv`, `file`, `fileset`, and `data.hcl_schema.<name>.url`
references for local schema-file workflows.
Supported Atlas env blocks can also set `schema.src`, `schema.mode`, `format`,
and local `diff` policy defaults for the `ptah-compat` binary's commands.
`ptah-compat migrate apply` expands env `for_each` collections and applies each
selected database target sequentially.

Ptah reads each selected project config once per command and converts it to a
typed configuration value. Migration database settings and online-DDL policy
therefore cannot come from different generations of a concurrently replaced
`ptah.yaml`. When an Atlas-compatible adapter delegates to a native command
that also consumes project configuration, the adapter passes the merged typed
snapshot instead of letting that command reopen either project file. This
currently applies to `migrate down`; other adapters evaluate Atlas project
configuration once and map supported values to explicit native command
arguments. An explicit `--config` path must exist; the conventional
`./ptah.yaml` is optional.

`--config` takes a `ptah.yaml` file. Pointing it at an `atlas.hcl` is refused by
name rather than reported as a YAML parse failure: the Atlas project config is
discovered as `./atlas.hcl` and selected with `--env`.

Every native command that accepts `--env` also accepts `--var name=value`, which
supplies a value for an `atlas.hcl` `variable` block that declares no default.
The flag is repeatable, and repeating one name builds a `list(string)`. This is
the flag the evaluator names when a variable cannot be resolved:

```text
$ ptah schema compare --env local --schema-file schema.sql
error: atlas.hcl variable "dburl" requires a default or --var dburl=value
$ ptah schema compare --env local --schema-file schema.sql --var dburl=sqlite://app.db
```

`env://` references resolve on the `ptah-compat` `--to` and `--from` flags. The
native `--schema-file` does not resolve them and says so, naming `env://` and,
for an attribute outside `src`, `schema.src`, `url`, `dev`, `migration`, and
`migration.dir`, naming the attribute.

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

If a command needs project values from a config file with multiple
environments, pass `--env`. Commands whose required inputs are all explicit do
not need to select an environment. Ptah fails instead of guessing only when
project values are required and the environment remains ambiguous.

## Operational settings

Project config can also define timeouts, revision table layout, migration
directory format, transaction mode, backup destinations, pre-flight hooks,
webhooks, lint defaults, and online-DDL policy.

The `dev` value supplies the disposable database for migration generation and
the shadow rehearsal that `migrations down` performs before touching its target.

| Setting area | Example keys |
| --- | --- |
| Database target | `url`, `src`, `schema.src`, `dev`, `schemas` |
| External desired schema | `external_schema.program`, `external_schema.format`, `external_schema.working_dir`, `external_schema.env` |
| Migration directory and revisions | `migration.dir`, `migration.format`, `migration.revisions_table`, `migration.revision_format` |
| Safety and operations | `migration.pre_up_hook`, `migration.pg_dump_to`, `migration.webhook`, `migration.exec_order`, `migration.tx_mode` |
| Lint defaults and policy | `lint.dialect`, `lint.disabled-rules`, `lint.latest`, `lint.git.base`, `lint.destructive.error`, `lint.concurrent_index.error` |
| Online DDL | `online_ddl.tool`, `online_ddl.threshold_rows`, `online_ddl.args`, `online_ddl.fallback` |
| Diff policy (native `migrations generate`) | `diff.skip: [drop_table, drop_column, drop_index, drop_enum]`, `diff.concurrent_index`, `diff.concurrent_index_drop` |
| Atlas-compatible output | `format.schema.inspect`, `format.schema.apply`, `format.schema.clean`, `format.schema.diff`, `format.migrate.apply`, `format.migrate.diff`, `format.migrate.lint`, `format.migrate.status` |
| Atlas-compatible diff policy | `diff.skip.drop_table`, `diff.concurrent_index.create`, `diff.concurrent_index.drop` |

The native `diff` block shapes what `ptah migrations generate` emits: `diff.skip`
lists destructive change kinds (`drop_table`, `drop_column`, `drop_index`,
`drop_enum`) to omit — a `-- SKIP: ...` comment is written in their place — and
`diff.concurrent_index: true` requests `CREATE INDEX CONCURRENTLY` for new
indexes (PostgreSQL, capability-gated), while
`diff.concurrent_index_drop: true` requests `DROP INDEX CONCURRENTLY` for
standalone index removals under the same gate. A skipped change is never
emitted, so it never trips the `--check-destructive` gate. A selected environment's
`diff.skip` replaces the top-level list; an explicit empty list clears all
inherited skip kinds.

The Atlas-compatible command tree lives in the separate `ptah-compat` binary,
the drop-in replacement for scripts that expect Atlas-style root commands.
Atlas project flags such as `--config`, `-c`, `--env`, and repeated
`--var name=value` belong to that tree only; it is not a separate
configuration surface.

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
Ptah config parsing is intentionally explicit. Unknown `ptah.yaml` keys and structurally unsupported `atlas.hcl` constructs fail with their source location. Names that Atlas CE accepts without acting on are the exception: Ptah accepts them for compatibility and warns that they have no effect. A rejected `ptah.yaml` key is reported by name, with its line and the keys that section accepts — never by the Go type the decoder was filling.
:::
