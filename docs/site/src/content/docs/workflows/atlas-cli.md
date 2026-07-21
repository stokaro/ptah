---
title: Atlas-compatible CLI
description: Use Atlas-style commands through the ptah atlas command tree.
---

Atlas-compatible command paths live under `ptah atlas <command> ...`.

Ptah does not add root-level Atlas spellings such as `ptah migrate apply` or `ptah schema inspect`. Those paths are intentionally invalid because the native Ptah command tree is being designed separately before GA.

## Migration commands

| Atlas-compatible command | Native Ptah command |
| --- | --- |
| `ptah atlas migrate apply` | `ptah migrations up` |
| `ptah atlas migrate down` | `ptah migrations down` |
| `ptah atlas migrate status` | `ptah migrations status` |
| `ptah atlas migrate hash` | `ptah migrations hash` |
| `ptah atlas migrate validate` | `ptah migrations validate` |
| `ptah atlas migrate lint` | `ptah migrations lint` |
| `ptah atlas migrate new` | `ptah migrations create` |
| `ptah atlas migrate diff` | `ptah migrations plan` or `ptah migrations generate`, depending on flags |

## Schema commands

| Atlas-compatible command | Native Ptah command |
| --- | --- |
| `ptah atlas schema inspect` | `ptah db read` |
| `ptah atlas schema diff` | `ptah schema compare` |

## Example

```bash
ptah atlas migrate apply \
  --url "$DATABASE_URL" \
  --dir ./migrations

ptah atlas schema inspect --url "$DATABASE_URL"
```

Ptah translates implemented Atlas-style flags and then delegates to native behavior. Unsupported Atlas flags should fail clearly instead of being ignored.
