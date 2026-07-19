# Ptah Project Config

`ptah.yaml` is Ptah's project-level configuration file. It is command
configuration, not a schema source. Schema input remains Go annotations, YAML
schema, Atlas schema HCL, or database introspection depending on the command.

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
    lint:
      dialect: postgres
      disabled-rules: [MF103]
    online_ddl:
      tool: ghost
      threshold_rows: 1000000
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
| `dev` | Disposable dev/shadow database URL for `migrate generate` |
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
| `lint.dialect` | Default lint dialect |
| `lint.disabled-rules` | Default lint disabled rule codes or families |
| `lint.latest` | Atlas-compatible project config value preserved in the IR |
| `online_ddl` | Automatic online-DDL routing config for MySQL/MariaDB |

`migrate.generate.shadow_db` is also accepted as the older spelling for `dev`.
When both are present, `dev` wins.

## Precedence

Runtime values resolve in this order:

1. Explicit CLI flags
2. Environment variables such as `PTAH_DB_URL`
3. `atlas.hcl`
4. `ptah.yaml`
5. Built-in command defaults

`atlas.hcl` is translated into the same project config IR. See
[Atlas Project Config](atlas_project_config.md) for the supported Atlas subset.
