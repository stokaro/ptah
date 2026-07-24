# Ptah Project Config

`ptah.yaml` is Ptah's project-level configuration file. It is command
configuration, not a schema source. Schema input remains Go annotations, YAML
schema, HCL schema files, or database introspection depending on the command.

Ptah reads `ptah.yaml` strictly: unknown keys are errors. This prevents a typo
from being silently ignored while migrations run with different settings than
the operator expected.

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
| `online_ddl` | Automatic online-DDL routing config for MySQL/MariaDB |
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

The list form is additive across environments: a named `env` block can add skip
kinds to those inherited from the top-level `diff.skip`, but it cannot remove an
inherited kind. Define skips at the level where they should apply.

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

`atlas.hcl` is translated into the same project config IR. Atlas-compatible
commands under `ptah atlas schema ...` and `ptah atlas migrate ...` also accept
Atlas project flags such as `--config`, `-c`, `--env`, and repeated
`--var name=value`. See [Atlas Project Config](atlas_project_config.md) for the
supported Atlas subset.
