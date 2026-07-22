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

Use `ptah.yaml` for Ptah-owned configuration and the supported `atlas.hcl` subset for Atlas-compatible project config.

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
| Database target | `url`, `dev`, `schemas` |
| Migration directory and revisions | `migration.dir`, `migration.format`, `migration.revisions_table`, `migration.revision_format` |
| Safety and operations | `migration.pre_up_hook`, `migration.pg_dump_to`, `migration.webhook`, `migration.exec_order`, `migration.tx_mode` |
| Lint defaults | `lint.dialect`, `lint.disabled-rules` |
| Online DDL | `online_ddl.tool`, `online_ddl.threshold_rows` |

References:

- [Project configuration](https://github.com/stokaro/ptah/blob/master/docs/project_config.md)
- [Atlas project config subset](https://github.com/stokaro/ptah/blob/master/docs/atlas_project_config.md)

:::note
Ptah config parsing is intentionally strict. Unknown `ptah.yaml` keys and unsupported `atlas.hcl` constructs fail instead of being ignored.
:::
