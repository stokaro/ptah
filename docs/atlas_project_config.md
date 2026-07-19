# Atlas Project Config

Ptah can read a limited Atlas project config subset from `atlas.hcl` and
translate it into Ptah's project config IR. This is project configuration for
commands, not schema HCL input. Schema HCL input is documented separately in
[Atlas HCL Schema Input](atlas_hcl_schema.md).

## Supported Subset

Ptah accepts top-level `env` blocks with either one label or no label:

```hcl
env "local" {
  url = "postgres://user:pass@localhost:5432/app?sslmode=disable"
  dev = "postgres://user:pass@localhost:5432/app_shadow?sslmode=disable"
  exclude = ["tmp_*"]

  migration {
    dir              = "file://migrations"
    format           = "atlas"
    revisions_schema = "atlas"
    lock_timeout     = "3s"
    exec_order       = "linear"
  }

  lint {
    latest = 5
  }
}
```

The supported attributes map to Ptah settings as follows:

| Atlas setting | Ptah setting |
| --- | --- |
| `env.url` | `--db-url` default |
| `env.dev` | `migrate generate --shadow-db` default |
| `env.exclude` | Project config IR exclude list |
| `migration.dir` | `--migrations-dir` or `--dir` default |
| `migration.format` | `--dir-format` default |
| `migration.revisions_schema` | `--migrations-schema` default |
| `migration.lock_timeout` | `--lock-timeout` default |
| `migration.exec_order` | `--exec-order` default |
| `lint.latest` | Project config IR lint setting |

`env.exclude` and `lint.latest` are preserved in the project config IR for
consumers that understand them. The current CLI wiring uses the connection,
dev database, migration directory, migration execution, revision-table, and
template env settings.

When an `atlas.hcl` `migration` block is present, Ptah also defaults
`revision-format` to `atlas`, so migration commands use
`atlas_schema_revisions` unless an explicit CLI flag overrides it. `file://`
migration directories are normalized to local paths. Other URI schemes are
rejected.

## Env Selection

Use `--env <name>` when an `atlas.hcl` file contains multiple `env` blocks.
When the file contains exactly one `env` block, Ptah selects it automatically.
If the file contains multiple envs and no `--env` is provided, Ptah returns:

```text
atlas.hcl contains multiple env blocks; pass --env
```

## Precedence

Ptah merges configuration in this order:

1. Explicit CLI flags
2. `atlas.hcl`
3. `ptah.yaml`
4. Built-in command defaults

This means a repo can keep an Atlas-shaped migration setup while still letting
one-off CLI invocations override any value:

```bash
ptah migrate-status --env local --json
ptah migrate-up --env local
ptah migrate-up --env local --db-url postgres://override/db
```

## Commands

The project config is currently consumed by commands that need the mapped
settings:

- `migrate-up`
- `migrate-down`
- `migrate-status`
- `lint`
- `migrate generate`

Atlas-compatible aliases under `ptah atlas <command> ...` inherit this behavior
when they forward to one of these native commands.

## Unsupported Constructs

Ptah intentionally rejects everything outside the documented subset. Unsupported
attributes, blocks, duplicate `migration` or `lint` blocks, dynamic expressions,
and non-file migration directory URI schemes fail with a location-aware error:

```text
unsupported atlas.hcl construct "src" at atlas.hcl:2
```

This hard-fail policy prevents partially interpreted Atlas project configs from
silently changing migration behavior.
