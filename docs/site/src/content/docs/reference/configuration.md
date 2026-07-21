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

References:

- [Project configuration](https://github.com/stokaro/ptah/blob/master/docs/project_config.md)
- [Atlas project config subset](https://github.com/stokaro/ptah/blob/master/docs/atlas_project_config.md)

:::note
Ptah config parsing is intentionally strict. Unknown `ptah.yaml` keys and unsupported `atlas.hcl` constructs fail instead of being ignored.
:::
