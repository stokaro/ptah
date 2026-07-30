---
title: Atlas project config
description: Supported `atlas.hcl` project configuration subset.
---

Ptah can read a strict subset of Atlas project configuration from `atlas.hcl`
and translate it into Ptah's project config IR. This is command configuration,
not schema HCL input. For schema HCL, see [HCL schema](../../reference/hcl-schema/).
`ptah-compat <command> ...` invocations on this page run the separate
`ptah-compat` drop-in binary; see the
[Atlas compatibility overview](../overview/).

## Supported blocks

Ptah accepts these local configuration blocks:

- top-level `variable`
- top-level `locals`
- `data "hcl_schema"` for local schema file data
- `env` blocks, with either one label or no label
- top-level and env-local `lint`
- env-local `schema`, `migration`, `format`, and `diff`

Atlas Cloud, registry, remote directory, and unsupported data-source constructs
fail explicitly.

## Example

```hcl
lint {
  git {
    base = "origin/master"
    dir  = "."
  }
}

env "local" {
  url     = "postgres://user:pass@localhost:5432/app?sslmode=disable"
  dev     = "postgres://user:pass@localhost:5432/app_shadow?sslmode=disable"
  src     = ["file://schema.hcl"]
  exclude = ["tmp_*"]

  schema {
    src = ["file://schema.hcl"]
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

## Mapping to Ptah behavior

| Atlas setting | Ptah behavior |
| --- | --- |
| `env.url` | Default database URL for compatible schema and migration commands. |
| `env.dev` | Default shadow/dev database URL where the command supports one. |
| `env.src` | Default desired schema source for `schema apply`. |
| `env.schema.src` | Default desired schema source for `schema apply`, `schema diff`, and `migrate diff`. |
| `env.schema.mode.<object>` | Default object-kind exclusions for supported schema object kinds. |
| `env.exclude` | Default Atlas-style resource exclusion filters. |
| `migration.dir` | Default migration directory. |
| `migration.format` | Default migration directory format where supported; safety gate for `migrate apply`. |
| `migration.revisions_schema` | Default revision metadata schema. |
| `migration.lock_timeout` | Default migration lock timeout. |
| `migration.exec_order` | Default migration execution order. |
| `migration.tx_mode` | Default transaction mode for compatible apply paths. |
| `lint.latest` | Latest-N migration lint selection. |
| `lint.git.base` | Git base for migration lint selection. |
| `lint.git.dir` | Git working directory for migration lint selection. |
| `lint.<analyzer>.error` | Severity mapping for supported Ptah lint rule families. |
| `lint.log` | Atlas Go-template that renders `migrate lint` output; shares the `format.migrate.lint` IR and precedence. |
| `format.schema.inspect` | Default `schema inspect --format`. |
| `format.schema.apply` | Default `schema apply --format`. |
| `format.schema.diff` | Default `schema diff --format`. |
| `format.migrate.apply` | Default `migrate apply --format`. |
| `format.migrate.diff` | Default `migrate diff --format`. |
| `format.migrate.lint` | Default `migrate lint --format`. |
| `format.migrate.status` | Default `migrate status --format`. |
| `diff.skip.drop_table` | Suppresses table drops in supported local diff/apply plans. |
| `diff.concurrent_index.create` | Requests PostgreSQL concurrent index creation where transaction mode allows it. |

Project config precedence is explicit CLI flags, environment variables,
`atlas.hcl`, `ptah.yaml`, then built-in defaults. Project-file merging
preserves source presence. For a supported field, an explicitly present value
replaces the lower-precedence value instead of being treated as absent. This
includes an empty string, zero, `false`, or an empty list when the field accepts
that type. After project sources are merged, a command applies its built-in
default only when a field is absent. An explicitly present empty or zero value
instead reaches normal validation. Fields that do not accept empty values,
including Atlas format templates, fail during parsing or command validation.
Forwarded commands pass this merged configuration snapshot to the native
implementation instead of reopening either project file.

`env.exclude` and disabled `env.schema.mode` values compose with the
`schema apply`/`schema diff` positive selection flags in a fixed order:
`--schema` names define the schema universe, `--include` selectors pick
resources inside it, and the configured exclusions subtract from that
selection last, exactly like CLI `--exclude`. See
[Scope the comparison with `--schema` and `--include`](../schema-commands/#scope-the-comparison-with---schema-and---include).

Ptah accepts Atlas's `atlas`, `golang-migrate`, `goose`, `flyway`, `liquibase`,
and `dbmate` values while evaluating `atlas.hcl`, and `ptah-compat migrate apply`
executes all of them. The native `atlas` format is read from disk unchanged
(preserving `atlas.sum` verification and down migrations); every other format is
converted in memory to Atlas single-file, up-only migrations, so apply runs only
the source tool's forward (up) SQL. Unknown formats and, currently, Flyway
repeatable migrations still fail before the target database is opened. An
explicit `?format=` query on the effective directory URL, whether declared by
`migration.dir` or passed with CLI `--dir`, overrides this project default,
matching Atlas's URL precedence. An empty query value selects the native
`atlas` format.

```bash
# Apply a Goose directory directly.
ptah-compat migrate apply --env local \
  --dir "file://migrations?format=goose"
```

Apply and `ptah-compat migrate import` share one format-loading implementation,
so they agree on every format's up/down semantics. See
[`stokaro/ptah#742`](https://github.com/stokaro/ptah/issues/742).

`env.src` and `env.schema.src` provide local schema-file defaults for
`schema apply` and `schema diff`. `migrate diff` resolves the same defaults
through its typed desired-state resolver, so they can contain local schema
files or one directly connectable database URL. Plain local schema paths and
relative `file://` schema URLs declared in `atlas.hcl` resolve relative to the
directory containing that `atlas.hcl` file, not the process working directory.
Explicit CLI `--to` and `--from` values keep CLI semantics and resolve relative
to the process working directory unless they are absolute.

The Atlas-compatible schema commands and `migrate diff` also accept explicit
`env://` references. `env://src` and `env://schema.src` expand the selected
environment's schema sources through the typed desired-schema resolver, so the
expanded value can be a supported local file or database URL. `env://url` and
`env://dev` resolve the corresponding database URL; `env://migration.dir`
resolves the configured local migration directory. Nested `env://` references
fail explicitly.

When an `atlas.hcl` `migration` block is present, Ptah defaults
`revision-format` to `atlas`, so migration commands use
`atlas_schema_revisions` unless an explicit CLI flag overrides it. Relative
`migration.dir` values declared in `atlas.hcl` resolve relative to the directory
containing that `atlas.hcl` file. Explicit CLI `--dir` values keep CLI semantics
and resolve relative to the process working directory unless they are absolute.
Non-local URI schemes in `migration.dir` and `schema.src` fail explicitly when a
command needs that configured value; an explicit CLI path flag still wins before
URI validation.

## Environment selection

Use Atlas project flags on commands under `ptah-compat schema ...` and
`ptah-compat migrate ...`:

```bash
ptah-compat schema inspect --config project.hcl --env local
ptah-compat migrate apply -c project.hcl --env local
ptah-compat migrate hash --env local --var dir=migrations
```

`--config` and `-c` select a local project config path. `file://` config URLs
are accepted; other URL schemes fail explicitly. `--var name=value` can be
repeated. Repeating the same variable name produces a string list for supported
Atlas HCL expressions. Variable overrides are strings; Atlas variable `type`
and `sensitive` attributes remain unsupported and fail explicitly.

If an `atlas.hcl` file has exactly one unnamed `env` block, Ptah can use it as
the default. Ambiguous or unsupported environment layouts fail instead of
guessing.

## Unsupported means error

Ptah intentionally rejects unsupported project config constructs. This prevents
a dangerous half-configured state where a user believes an Atlas setting is in
effect but Ptah silently ignored it.
