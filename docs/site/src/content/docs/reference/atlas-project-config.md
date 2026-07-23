---
title: Atlas Project Config
description: Supported `atlas.hcl` project configuration subset.
---

Ptah can read a strict subset of Atlas project configuration from `atlas.hcl`
and translate it into Ptah's project config IR. This is command configuration,
not schema HCL input. For schema HCL, see [HCL schema](../hcl-schema/).

## Supported Blocks

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

## Mapping To Ptah Behavior

| Atlas setting | Ptah behavior |
| --- | --- |
| `env.url` | Default database URL for compatible schema and migration commands. |
| `env.dev` | Default shadow/dev database URL where the command supports one. |
| `env.src` | Default desired schema source for `schema apply`. |
| `env.schema.src` | Default desired schema source for `schema apply`, `schema diff`, and `migrate diff`. |
| `env.schema.mode.<object>` | Default object-kind exclusions for supported schema object kinds. |
| `env.exclude` | Default Atlas-style resource exclusion filters. |
| `migration.dir` | Default migration directory. |
| `migration.format` | Default migration directory format. |
| `migration.revisions_schema` | Default revision metadata schema. |
| `migration.lock_timeout` | Default migration lock timeout. |
| `migration.exec_order` | Default migration execution order. |
| `migration.tx_mode` | Default transaction mode for compatible apply paths. |
| `lint.latest` | Latest-N migration lint selection. |
| `lint.git.base` | Git base for migration lint selection. |
| `lint.git.dir` | Git working directory for migration lint selection. |
| `lint.<analyzer>.error` | Severity mapping for supported Ptah lint rule families. |
| `format.schema.inspect` | Default `schema inspect --format`. |
| `format.schema.apply` | Default `schema apply --format`. |
| `format.schema.diff` | Default `schema diff --format`. |
| `format.migrate.apply` | Default `migrate apply --format`. |
| `format.migrate.diff` | Default `migrate diff --format`. |
| `diff.skip.drop_table` | Suppresses table drops in supported local diff/apply plans. |
| `diff.concurrent_index.create` | Requests PostgreSQL concurrent index creation where transaction mode allows it. |

Explicit CLI flags win over `atlas.hcl`, and `atlas.hcl` wins over built-in
defaults.

## Environment Selection

Use `--env` on Atlas-compatible commands:

```bash
ptah atlas schema inspect --env local
ptah atlas migrate apply --env local
```

If an `atlas.hcl` file has exactly one unnamed `env` block, Ptah can use it as
the default. Ambiguous or unsupported environment layouts fail instead of
guessing.

## Unsupported Means Error

Ptah intentionally rejects unsupported project config constructs. This prevents
a dangerous half-configured state where a user believes an Atlas setting is in
effect but Ptah silently ignored it.
