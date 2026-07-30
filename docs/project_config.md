# Ptah Project Config

`ptah.yaml` is Ptah's project-level configuration file. It selects command
defaults and can select an external desired-schema program; the program's
stdout is the schema source. Static schema input remains Go annotations, YAML,
HCL, or SQL files, with database introspection used by commands that compare
against a live target.

Ptah reads `ptah.yaml` strictly: unknown keys are errors. This prevents a typo
from being silently ignored while migrations run with different settings than
the operator expected.

Each command reads `ptah.yaml` once into the typed project configuration IR.
Database settings, migration settings, and online-DDL execution policy
therefore come from the same file generation. Atlas-compatible command adapters
pass the merged IR to the native command implementation instead of reopening
`ptah.yaml` or `atlas.hcl`. An explicit `--config` path must exist; the
conventional `./ptah.yaml` remains optional.

## Named Environments

Use `env` blocks to name reusable database targets:

```yaml
env:
  prod:
    url: postgres://user:pass@prod-host:5432/app
    dev: postgres://user:pass@localhost:5432/app_shadow
    schemas: [public]
    migration:
      dir: ./migrations
      format: atlas
      revisions_schema: atlas
      revisions_table: atlas_schema_revisions
      revision_format: atlas
      lock_timeout: 3s
      statement_timeout: 30s
      connect_timeout: 10s
      migration_lock_timeout: 15s
      exec_order: linear
      tx_mode: file
      pre_up_hook: ./scripts/backup-before-up
      pre_down_hook: ./scripts/backup-before-down
      pg_dump_to: ./backups/postgres
      mysqldump_to: ./backups/mysql
      webhook: https://ops.example/hooks/ptah-migration
    lint:
      dialect: postgres
      disabled-rules: [MF103]
    online_ddl:
      tool: ghost
      threshold_rows: 1000000
    diff:
      skip: [drop_table, drop_column]
      concurrent_index: true
```

Select an environment with `--env <name>` on commands that load project
configuration. If `ptah.yaml` contains exactly one environment, Ptah selects it
automatically. If it contains multiple environments and no `--env` is passed,
Ptah fails instead of guessing.

Top-level settings are allowed and are merged as defaults for every named
environment:

```yaml
migration:
  exec_order: linear

env:
  dev:
    url: postgres://localhost/dev
  prod:
    url: postgres://prod/app
    migration:
      exec_order: non-linear
```

## Supported Keys

| Key | Meaning |
| --- | --- |
| `url` | Default target database URL for migration commands |
| `dev` | Disposable dev/shadow database URL for `migrations generate` |
| `schemas` | Default schemas to introspect when the command supports schema scoping |
| `exclude` | Project-level exclude patterns for config consumers |
| `external_schema.program` | External schema command as an explicit argument list; the first item is the executable |
| `external_schema.format` | Command stdout format: `sql` (default), `hcl`, or `yaml` |
| `external_schema.working_dir` | Working directory for the external schema command |
| `external_schema.env` | Extra `KEY=VALUE` environment entries appended for the command |
| `migration.dir` | Default migrations directory |
| `migration.format` | Migration directory format: `auto`, `ptah`, or `atlas` |
| `migration.revisions_schema` | Migration metadata schema |
| `migration.revisions_table` | Migration metadata table |
| `migration.revision_format` | Revision table layout: `ptah` or `atlas` |
| `migration.lock_timeout` | Default per-migration lock timeout |
| `migration.statement_timeout` | Default per-migration statement timeout |
| `migration.connect_timeout` | Initial database connection timeout |
| `migration.migration_lock_timeout` | Session-level migration advisory lock timeout |
| `migration.exec_order` | Pending migration execution policy |
| `migration.tx_mode` | Migration transaction mode: `file`, `all`, or `none` |
| `migration.pre_up_hook` | Shell command that must succeed before `migrations up` changes the schema |
| `migration.pre_down_hook` | Shell command that must succeed before `migrations down` changes the schema |
| `migration.pg_dump_to` | Directory for a PostgreSQL-compatible pre-migration custom-format dump |
| `migration.mysqldump_to` | Directory for a MySQL/MariaDB pre-migration SQL dump |
| `migration.webhook` | URL that receives migration metadata before `migrations up` or `migrations down`; it must return HTTP 200 |
| `lint.dialect` | Default lint dialect |
| `lint.disabled-rules` | Default lint disabled rule codes or families |
| `lint.latest` | Default latest-version changeset for `migrations lint` |
| `online_ddl.tool` | Automatic online-DDL tool for MySQL/MariaDB: `ghost` or `pt-osc` |
| `online_ddl.threshold_rows` | Estimated row threshold that activates automatic routing |
| `online_ddl.args` | Extra arguments appended to every online-DDL tool invocation |
| `online_ddl.fallback` | Routing fallback policy: `error` or `plain` |
| `diff.skip` | Destructive change kinds the planner omits from generated migrations (`drop_table`, `drop_column`, `drop_index`, `drop_enum`) |
| `diff.concurrent_index` | Emit `CREATE INDEX CONCURRENTLY` for newly added indexes (PostgreSQL, capability-gated) |

`migrate.generate.shadow_db` is also accepted as the older spelling for `dev`.
When both are present, `dev` wins.

Custom pre-flight hook commands receive the raw `PTAH_DB_URL`, `PTAH_DIALECT`,
`PTAH_CURRENT_VERSION`, and `PTAH_TARGET_VERSION` environment variables.
`pg_dump_to` writes files named `ptah_pre_v{from}_to_v{to}_{ts}.dump`, and
`mysqldump_to` writes `ptah_pre_v{from}_to_v{to}_{ts}.sql`, with a
high-precision UTC timestamp. Webhooks have a 30-second timeout and redirects
are not followed. Dry-run migration commands do not execute hooks because
backups and webhooks are side effects.

## External Desired Schema

Use `external_schema` when an ORM, framework, or generator owns the desired
schema:

```yaml
external_schema:
  program:
    - .venv/bin/atlas-provider-sqlalchemy
    - --path
    - ./models
    - --dialect
    - postgresql
  format: sql
  working_dir: ./app
  env: ["APP_ENV=dev"]
```

`program` is an explicit argument list. Ptah executes it directly without a
shell, bounds it with the external-schema timeout, reads stdout as `sql`, `hcl`,
or `yaml`, and surfaces bounded, secret-redacted, terminal-safe stderr and
parser diagnostics when the program fails. Empty or whitespace-only stdout is
rejected.
`working_dir` defaults to the process working directory; `env` entries are
appended to the current environment. `PATH` and `PWD` cannot be overridden:
use an explicit executable path in `program` and `working_dir` for the command
directory.

The block applies consistently to `ptah schema render`, `ptah schema compare`,
`ptah schema drift`, `ptah migrations plan`, and
`ptah migrations generate`. These commands use it only when `--schema-cmd` is
not set. When `--schema-cmd` is set, its `--schema-format` keeps CLI precedence
over the config block. An explicit empty `--schema-cmd=` disables the configured
external source. All commands honor `--config` and `--env` for this source.

Because `./ptah.yaml` is auto-discovered, merely entering a repository and
running a schema command must not execute repository-controlled code. Pass
`--allow-external-schema` to execute a config-sourced program:

```bash
ptah schema render --allow-external-schema --dialect postgres
```

Supplying `--schema-cmd` is already an explicit opt-in and does not require the
additional flag. Relative `working_dir` values must remain inside the current
working directory after symlink resolution; use an explicit absolute path when
the loader intentionally lives elsewhere. Ptah terminates the loader's complete
process group on Unix and its kill-on-close Job Object on Windows, including
descendants left behind after a successful parent exit.

See the canonical [ORM and external loaders](site/src/content/docs/schema/orm-and-external.md)
page and [composite desired schema](site/src/content/docs/schema/composite.md) rules.

## Diff Policy

The `diff` block declaratively controls which changes `migrations generate`
emits, so a project can shape generated migrations without editing Go code or
hand-patching SQL. This mirrors Atlas's open-source `diff { skip { ... }
concurrent_index { ... } }` policy.

```yaml
diff:
  skip: [drop_table, drop_column, drop_index, drop_enum]
  concurrent_index: true
```

**`diff.skip`** lists destructive change kinds to omit from the plan. A skipped
change is not emitted at all — a clearly-marked comment is written in its place,
for example:

```sql
-- SKIP: DROP TABLE of legacy_events omitted by diff policy (skip: drop_table)
```

Supported kinds: `drop_table`, `drop_column`, `drop_index`, `drop_enum`.
Skipping `drop_table` also omits the dependent removals (indexes, constraints,
triggers, RLS policies, table-level grants) that a kept table must retain, so the
plan stays consistent. Skip is currently honored by the PostgreSQL-family planner.

The selected environment's `diff.skip` replaces the top-level list when the
field is present. An explicit empty list therefore clears inherited skip kinds.

This is finer-grained than the coarse `--check-destructive` / `--allow-destructive`
gate: `--check-destructive` blocks (or allows) the whole migration when it
contains any destructive statement, whereas `diff.skip` removes specific
destructive kinds from the migration entirely. Because a skipped change is never
emitted, it also never trips the destructive gate — the two features compose:
skip the drops you never want, and gate on whatever destructive changes remain.
The paired down migration is filtered the same way, so a skipped `drop_table`
does not become a `CREATE TABLE` on rollback.

Because a skipped drop deliberately leaves an object the Go schema no longer
declares, `skip` intentionally diverges the database from the desired schema. If
you also run `migrations generate` with shadow verification (`--shadow-db` /
`dev`), that verification compares the replayed result against the Go schema and
will report the retained object as drift. Use `skip` without shadow verification,
or reconcile the retained objects out of band.

**`diff.concurrent_index: true`** requests `CREATE INDEX CONCURRENTLY` for every
newly added index, superseding the built-in heuristic (which otherwise only
builds indexes on already-populated tables concurrently). It remains gated on
the target's capabilities: a PostgreSQL-compatible engine without concurrent
index support keeps plain `CREATE INDEX`. Concurrent index builds cannot run
inside a transaction, so the affected statements are split into a
`+ptah no_transaction` migration file automatically.

## Precedence

Runtime values resolve in this order:

1. Explicit CLI flags
2. Environment variables such as `PTAH_DB_URL`
3. `atlas.hcl`
4. `ptah.yaml`
5. Built-in command defaults

Project-file merging preserves source presence. For a supported field, an
explicitly present value replaces the lower-precedence value instead of being
treated as absent. This includes an empty string, zero, `false`, or an empty
list when the field accepts that type. Thus `atlas.hcl` wins over `ptah.yaml`,
while environment variables and explicit CLI flags still win.
After project sources are merged, a command applies its built-in default only
when a field is absent. An explicitly present empty or zero value instead
reaches the command's normal validation. Fields that do not accept empty values
fail during parsing or command validation.

`atlas.hcl` is translated into the same project config IR. The `ptah-compat`
binary's Atlas-compatible `schema ...` and `migrate ...` commands also accept
Atlas project flags such as `--config`, `-c`, `--env`, and repeated
`--var name=value`. See [Atlas Project Config](atlas_project_config.md) for the
supported Atlas subset.
