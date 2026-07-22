# Atlas Project Config

Ptah can read a limited Atlas project config subset from `atlas.hcl` and
translate it into Ptah's project config IR. This is project configuration for
commands, not schema HCL input. Schema HCL input is documented separately in
[HCL Schema Input](atlas_hcl_schema.md).

## Supported Subset

Ptah accepts top-level `variable`, `locals`, `data "hcl_schema"`, `env`, and
`lint` blocks. `env` blocks may have either one label or no label:

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
local schema file sources only, matching the local schema-file boundary of
`ptah atlas schema apply`.

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

## Expression Evaluation

Ptah evaluates a scoped Atlas-compatible expression subset for local project
config workflows:

- `variable` blocks with `default` values and optional `description` metadata.
- `locals` blocks referenced as `local.<name>`.
- `getenv("NAME")` for environment-provided URLs and settings.
- `file("path")` for local file contents, relative to the `atlas.hcl` file.
- `fileset("glob")` for local file lists, relative to the `atlas.hcl` file.
- `data "hcl_schema" "name"` blocks with either `path` or `paths`, exposed as
  `data.hcl_schema.<name>.url`.

Example:

```hcl
variable "database_url" {
  default = getenv("DATABASE_URL")
}

data "hcl_schema" "app" {
  paths = fileset("schema/*.hcl")
}

env "local" {
  url = var.database_url
  src = data.hcl_schema.app.url
  dev = getenv("DEV_DATABASE_URL")
}
```

`data.hcl_schema.<name>.url` returns one `file://...` URL when `path` is used
and a list of `file://...` URLs when `paths` is used. `fileset` returns stable
slash-separated relative paths sorted lexicographically and supports recursive
`**` path segments.

Ptah does not expose Atlas CLI variable override flags yet. A `variable` block
therefore needs a `default` value. Variable `type` and `sensitive` attributes
are not accepted until Ptah implements their semantics. Unsupported dynamic data
sources such as external schemas, SQL data sources, registry-backed sources, and
Cloud-specific sources still fail explicitly.

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
attributes, unsupported data sources, unsupported lint policy/analyzer blocks,
duplicate `migration` or `lint` blocks, variables without defaults, and non-file
migration directory URI schemes fail with a location-aware error:

```text
unsupported atlas.hcl construct "src" at atlas.hcl:2
```

This hard-fail policy prevents partially interpreted Atlas project configs from
silently changing migration behavior.
