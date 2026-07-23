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

Use `ptah.yaml` for Ptah-owned configuration and the supported `atlas.hcl`
subset for Atlas-compatible project config. The supported Atlas subset includes
local `variable` defaults and Atlas-style `--var name=value` overrides,
`locals`, `getenv`, `file`, `fileset`, and `data.hcl_schema.<name>.url`
references for local schema-file workflows.
Supported Atlas env blocks can also set `schema.src`, `schema.mode`, `format`,
and local `diff` policy defaults for `ptah atlas ...` commands.

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

If a config file has multiple environments, pass `--env`. Ptah fails instead of
guessing.

## Operational settings

Project config can also define timeouts, revision table layout, migration
directory format, transaction mode, backup destinations, pre-flight hooks,
webhooks, lint defaults, and online-DDL policy.

| Setting area | Example keys |
| --- | --- |
| Database target | `url`, `src`, `schema.src`, `dev`, `schemas` |
| Migration directory and revisions | `migration.dir`, `migration.format`, `migration.revisions_table`, `migration.revision_format` |
| Safety and operations | `migration.pre_up_hook`, `migration.pg_dump_to`, `migration.webhook`, `migration.exec_order`, `migration.tx_mode` |
| Lint defaults and policy | `lint.dialect`, `lint.disabled-rules`, `lint.latest`, `lint.git.base`, `lint.destructive.error`, `lint.concurrent_index.error` |
| Online DDL | `online_ddl.tool`, `online_ddl.threshold_rows` |
| Atlas-compatible output | `format.schema.inspect`, `format.schema.apply`, `format.schema.diff`, `format.migrate.apply`, `format.migrate.diff` |
| Atlas-compatible diff policy | `diff.skip.drop_table`, `diff.concurrent_index.create` |

The Atlas-compatible command tree lives under `ptah atlas <command> ...`.
Atlas project flags such as `--config`, `-c`, `--env`, and repeated
`--var name=value` belong to this tree only.
`ptah-compat` is the drop-in replacement binary for scripts that expect
Atlas-style root commands; it is not a separate configuration surface.

Continue with [Atlas project config](../atlas-project-config/) for the supported
`atlas.hcl` subset.

:::note
Ptah config parsing is intentionally strict. Unknown `ptah.yaml` keys and unsupported `atlas.hcl` constructs fail instead of being ignored.
:::
