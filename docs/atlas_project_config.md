# Atlas Project Config

Ptah can read a limited Atlas project config subset from `atlas.hcl` and
translate it into Ptah's project config IR. This is project configuration for
commands, not schema HCL input. Schema HCL input is documented separately in
[HCL Schema Input](atlas_hcl_schema.md).

## Supported Subset

Ptah accepts top-level `env` and `lint` blocks. `env` blocks may have either
one label or no label:

```hcl
lint {
  git {
    base = "origin/master"
    dir  = "."
  }
}

env "local" {
  url = "postgres://user:pass@localhost:5432/app?sslmode=disable"
  dev = "postgres://user:pass@localhost:5432/app_shadow?sslmode=disable"
  src = ["file://schema.hcl", "schema.sql"]
  exclude = ["tmp_*"]

  migration {
    dir              = "file://migrations"
    format           = "atlas"
    revisions_schema = "atlas"
    lock_timeout     = "3s"
    exec_order       = "linear"
    tx_mode          = "file"
  }

  lint {
    latest = 5
  }
}
```

The supported attributes map to Ptah settings as follows:

| Atlas setting | Ptah setting |
| --- | --- |
| `env.url` | `--db-url` or `ptah atlas schema apply --url` default |
| `env.dev` | `migrations generate --shadow-db` or `ptah atlas schema apply --dev-url` default |
| `env.src` | `ptah atlas schema apply --to` default |
| `env.exclude` | `ptah atlas schema apply --exclude` default |
| `migration.dir` | `--migrations-dir` or `--dir` default |
| `migration.format` | `--dir-format` default |
| `migration.revisions_schema` | `--migrations-schema` default |
| `migration.lock_timeout` | `--lock-timeout` default |
| `migration.exec_order` | `--exec-order` default |
| `migration.tx_mode` | `migrations up --tx-mode` default |
| `lint.latest` | `migrations lint --latest` default |
| `lint.git.base` | `migrations lint --git-base` default |
| `lint.git.dir` | `migrations lint --git-dir` default |

`env.src` accepts either one string or a list of strings. Ptah currently uses
literal local schema file sources only, matching the local schema-file boundary
of `ptah atlas schema apply`. Data-source expressions such as
`data.hcl_schema.app.url` are rejected until Ptah implements HCL expression
evaluation for project config files.

`env.exclude` accepts either one string or a list of strings. `ptah atlas schema
apply --env <name>` uses it as the default resource exclusion filter unless an
explicit `--exclude` flag is provided.

`lint.latest` and `lint.git` configure the migration changeset selected by
`migrations lint` and `ptah atlas migrate lint`. These selectors are mutually
exclusive. `lint.git.dir` matches Atlas's working-directory option for Git
changeset detection and defaults to the current directory when omitted.

`migration.tx_mode` accepts `file`, `all`, and `none`, matching
`ptah atlas migrate apply --tx-mode`. `all` is limited to dialects where Ptah
can safely wrap DDL in a single transaction and conflicts with file-level
`no_transaction` directives. `none` intentionally rejects migration timeouts
because Ptah does not yet apply timeout setup and restore through a dedicated
single-session executor.

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
2. `PTAH_*` environment variables
3. `atlas.hcl`
4. `ptah.yaml`
5. Built-in command defaults

This means a repo can keep an Atlas-shaped migration setup while still letting
one-off CLI invocations override any value:

```bash
ptah migrations status --env local --json
ptah migrations up --env local
ptah migrations up --env local --db-url postgres://override/db
```

## Commands

The project config is currently consumed by commands that need the mapped
settings:

- `migrations up`
- `migrations down`
- `migrations status`
- `migrations lint`
- `migrations generate`
- `ptah atlas schema apply`

Atlas command paths under `ptah atlas <command> ...` inherit this behavior
when they forward to one of these native commands. Dedicated Atlas-compatible
commands document their own project-config support. `ptah atlas schema apply`
reads `env.url`, `env.src`, `env.dev`, and `env.exclude` from the selected
`atlas.hcl` environment without coupling that selection to `ptah.yaml` env
names.

## Unsupported Constructs

Ptah intentionally rejects everything outside the documented subset. Unsupported
attributes, unsupported lint policy/analyzer blocks, duplicate `migration` or
`lint` blocks, dynamic expressions, and non-file migration directory URI schemes
fail with a location-aware error:

```text
unsupported atlas.hcl construct "src" at atlas.hcl:2
```

This hard-fail policy prevents partially interpreted Atlas project configs from
silently changing migration behavior.
