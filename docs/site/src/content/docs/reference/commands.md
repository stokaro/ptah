---
title: Commands
description: Native and Atlas-compatible Ptah command surfaces.
---

Ptah has two command surfaces:

- Native Ptah commands, owned by Ptah.
- Atlas-compatible commands, reserved under `ptah atlas <command> ...`.

Reference: [native CLI command tree](https://github.com/stokaro/ptah/blob/master/docs/native_cli.md).

## Native commands

| Command | Purpose |
| --- | --- |
| `ptah introspect` | Generate annotated Go models from a live database. |
| `ptah schema render` | Render desired schema SQL from Go, YAML, or Atlas HCL inputs. |
| `ptah schema compare` | Compare desired schema with a live database. |
| `ptah schema drift` | Check live database drift against desired schema. |
| `ptah schema export` | Export one schema source format to another. |
| `ptah viz` | Render desired schema diagrams as Mermaid, DOT, or SVG. |
| `ptah db read` | Read schema from a live database. |
| `ptah migrations plan` | Print migration SQL from desired/live schema differences. |
| `ptah migrations generate` | Generate migration files from desired/live schema differences. |
| `ptah migrations create` | Create empty migration files for manual SQL. |
| `ptah migrations up` | Run pending migrations. |
| `ptah migrations down` | Roll back migrations. |
| `ptah migrations status` | Show migration status. |
| `ptah migrations hash` | Write or update migration-directory integrity. |
| `ptah migrations validate` | Validate migration-directory integrity. |
| `ptah migrations lint` | Lint migration files. |

## Atlas-compatible commands

Use `ptah atlas ...` only. Root-level Atlas aliases are intentionally absent.

```bash
ptah atlas migrate apply --url "$DATABASE_URL" --dir ./migrations
ptah atlas migrate status --url "$DATABASE_URL" --dir ./migrations
ptah atlas schema inspect --url "$DATABASE_URL"
```
