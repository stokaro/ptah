# Atlas Project Config

Ptah can read a limited Atlas project config subset from `atlas.hcl` and
translate it into Ptah's project config IR. This is project configuration for
commands, not schema HCL input. Schema HCL input is documented separately in
[HCL Schema Input](atlas_hcl_schema.md).

## Supported Subset

Ptah accepts top-level `variable`, `locals`, `data "hcl_schema"`, `env`,
`lint`, and `diff` blocks. `env` blocks may have either one label or no label:

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

  schema {
    src = ["file://schema.hcl", "schema.sql"]
    mode {
      funcs       = false
      permissions = false
      roles       = false
      triggers    = false
    }
  }

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
    destructive {
      error = false
    }
    concurrent_index {
      error = true
    }
  }

  format {
    schema {
      inspect = "json"
      apply   = "{{ sql . \"  \" }}"
      diff    = "{{ sql . \"\" }}"
    }
    migrate {
      apply = "{{ json . }}"
      diff  = "{{ sql . \"\" }}"
    }
  }

  diff {
    skip {
      drop_table = true
    }
    concurrent_index {
      create = true
    }
  }
}
```

The supported attributes map to Ptah settings as follows:

| Atlas setting | Ptah setting |
| --- | --- |
| `env.url` | `--db-url`, `ptah atlas schema inspect --url`, `ptah atlas schema apply --url`, `ptah atlas migrate apply --url`, or `ptah atlas migrate status --url` default |
| `env.dev` | `migrations generate --shadow-db`, `ptah atlas schema inspect --dev-url`, `ptah atlas schema apply --dev-url`, `ptah atlas schema diff --dev-url`, `ptah atlas migrate diff --dev-url`, or `ptah atlas migrate lint --dev-url` default |
| `env.src` | `ptah atlas schema apply --to` default |
| `env.schema.src` | `ptah atlas schema apply --to`, `ptah atlas schema diff --to`, or `ptah atlas migrate diff --to` default |
| `env.schema.mode.<object>` | Atlas-style exclusion defaults for supported object kinds |
| `env.exclude` | `ptah atlas schema inspect --exclude`, `ptah atlas schema apply --exclude`, or `ptah atlas schema diff --exclude` default |
| `migration.dir` | `--migrations-dir` or `--dir` default |
| `migration.format` | `--dir-format` default |
| `migration.revisions_schema` | `--migrations-schema` default |
| `migration.lock_timeout` | `--lock-timeout` default |
| `migration.exec_order` | `--exec-order` default |
| `migration.tx_mode` | `migrations up --tx-mode` default |
| `lint.latest` | `migrations lint --latest` default |
| `lint.git.base` | `migrations lint --git-base` default |
| `lint.git.dir` | `migrations lint --git-dir` default |
| `lint.destructive.error` | `DS` lint rule-family severity |
| `lint.concurrent_index.error` | `PG101` and `PG103` lint rule severity |
| `lint.data_depend.error` | `DD` lint rule-family severity |
| `lint.incompatible.error` | `BC` lint rule-family severity |
| `lint.nestedtx.error` | `TX201` lint rule severity |
| `format.schema.inspect` | `ptah atlas schema inspect --format` default |
| `format.schema.apply` | `ptah atlas schema apply --format` default |
| `format.schema.clean` | `ptah atlas schema clean --format` default |
| `format.schema.diff` | `ptah atlas schema diff --format` default |
| `format.migrate.apply` | `ptah atlas migrate apply --format` default |
| `format.migrate.diff` | `ptah atlas migrate diff --format` default |
| `format.migrate.lint` | `ptah atlas migrate lint --format` default |
| `format.migrate.status` | `ptah atlas migrate status --format` default |
| `diff.skip.drop_table` | Drop-table suppression for local schema diff/apply planning |
| `diff.concurrent_index.create` | PostgreSQL concurrent index creation where the command can execute without a surrounding transaction |

`env.src` and `env.schema.src` accept either one string or a list of strings.
The nested `schema.src` form matches Atlas project config syntax. Ptah currently
uses local schema file sources only, matching the local schema-file boundary of
`ptah atlas schema apply`, `ptah atlas schema diff`, and
`ptah atlas migrate diff`.

`env.exclude` accepts either one string or a list of strings. `ptah atlas schema
apply --env <name>` uses it as the default resource exclusion filter unless an
explicit `--exclude` flag is provided.

`env.schema.mode` accepts `funcs`, `objects`, `permissions`, `roles`, `tables`,
`triggers`, `types`, and `views` booleans. Ptah maps disabled values to the
matching Atlas-style resource exclusions for object kinds represented in Ptah's
schema IR. `sensitive = DENY` is accepted as a no-op because Ptah does not emit
sensitive values through the supported local workflows. `sensitive = ALLOW` is
rejected until Ptah has explicit sensitive-value semantics.

`format` blocks configure the same Atlas Go-template output strings accepted by
the matching commands. Ptah supports `schema.inspect`, `schema.apply`,
`schema.diff`, `migrate.apply`, `migrate.diff`, `migrate.lint`, and
`migrate.status` for the command-specific output contracts documented in the
Atlas-compatible command reference.

`diff.skip.drop_table = true` removes table drops from supported local
declarative diff/apply plans and also removes index or constraint drops owned by
those dropped tables. `diff.skip.drop_schema` is rejected because Ptah does not
currently model schema dropping as an Atlas-compatible policy decision.

`diff.concurrent_index.create = true` maps to PostgreSQL concurrent index
creation in schema diff planning. For non-dry-run PostgreSQL `schema apply`
plans that actually emit `CREATE INDEX CONCURRENTLY`, Ptah requires
`--tx-mode none` because PostgreSQL does not allow concurrent index creation
inside a transaction. `ptah atlas migrate diff` rejects this policy for now
because generated migration files do not yet carry the required no-transaction
metadata. `diff.concurrent_index.drop` is rejected until Ptah implements
matching concurrent drop semantics.

`lint.latest` and `lint.git` configure the migration changeset selected by
`migrations lint` and `ptah atlas migrate lint`. These selectors are mutually
exclusive. `lint.git.dir` matches Atlas's working-directory option for Git
changeset detection and defaults to the current directory when omitted.

The supported lint policy analyzer blocks map the Atlas `error` boolean to
Ptah lint severity only where the analyzer has a matching Ptah rule family.
`error = true` sets the mapped findings to error severity; `error = false`
sets them to warning severity. The supported mappings are `destructive` to the
`DS` family, `data_depend` to the `DD` family, `incompatible` to the `BC`
family, `concurrent_index` to `PG101` and `PG103`, and `nestedtx` to `TX201`.
Atlas `check` blocks are rejected for now because Atlas check IDs and Ptah rule
IDs are not a stable one-to-one namespace. Analyzer `force` options, allow-list
blocks such as `allow_table` / `allow_column`, custom `rule` blocks, and
policy families without a matching Ptah lint engine fail explicitly.

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
- `format(format_string, values...)` and `jsonencode(value)` for Atlas-style
  local project-config string construction.

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

Atlas-compatible commands under `ptah atlas schema ...` and
`ptah atlas migrate ...` accept Atlas project flags:

- `--config path/to/atlas.hcl` or `-c path/to/atlas.hcl` selects the project
  config file. Ptah also accepts local `file://` config URLs. Other URL schemes
  fail explicitly.
- `--env <name>` selects a named Atlas environment.
- `--var name=value` provides a variable override. The flag can be repeated;
  repeated values for the same variable become a string list, matching Atlas's
  local project-variable behavior.

Variable overrides are strings. A `variable` block without a `default` is valid
when the invocation provides a matching `--var name=value`. Variable `type` and
`sensitive` attributes are not accepted until Ptah implements their semantics.
Unsupported dynamic data sources such as external schemas, SQL data sources,
registry-backed sources, and Cloud-specific sources still fail explicitly.

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
- `ptah atlas schema inspect`
- `ptah atlas schema apply`
- `ptah atlas schema diff`
- `ptah atlas migrate apply`
- `ptah atlas migrate diff`
- `ptah atlas migrate lint`

Atlas command paths under `ptah atlas <command> ...` inherit this behavior
when they forward to one of these native commands. Dedicated Atlas-compatible
commands document their own project-config support. The separate `ptah-compat`
binary exposes the same Atlas-compatible command tree at process root for
drop-in script migration.

`ptah atlas schema inspect` reads `env.url`, `env.dev`, `env.exclude`,
`env.schema.mode`, and `format.schema.inspect`.

`ptah atlas schema apply` reads `env.url`, `env.src`, `env.schema.src`,
`env.dev`, `env.exclude`, `env.schema.mode`, `format.schema.apply`, and
supported `diff` policy.

`ptah atlas schema diff` reads `env.schema.src`, `env.dev`, `env.exclude`,
`env.schema.mode`, `format.schema.diff`, and supported `diff` policy.

`ptah atlas schema clean` reads `env.url` and `format.schema.clean`.

`ptah atlas migrate apply` reads `env.url`, `migration`, and
`format.migrate.apply`.

`ptah atlas migrate diff` reads `env.schema.src`, `env.dev`, `migration.dir`,
`format.migrate.diff`, and supported `diff` policy.

## Unsupported Constructs

Ptah intentionally rejects everything outside the documented subset. Unsupported
attributes, unsupported data sources, unsupported lint policy blocks or attributes,
Cloud or registry sources such as `schema.repo`, unsupported format blocks,
unsupported diff policy fields, duplicate `migration` or `lint` blocks,
variables without defaults that are not supplied through `--var`, and non-file
migration directory URI schemes fail with a location-aware error:

```text
unsupported atlas.hcl construct "src" at atlas.hcl:2
```

This hard-fail policy prevents partially interpreted Atlas project configs from
silently changing migration behavior.
