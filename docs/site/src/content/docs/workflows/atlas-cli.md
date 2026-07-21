---
title: Atlas-compatible CLI
description: Use Atlas-style commands through the ptah atlas command tree.
---

Atlas-compatible command paths live under `ptah atlas <command> ...`.

Ptah does not add root-level Atlas spellings such as `ptah migrate apply` or `ptah schema inspect`. Those paths are intentionally invalid because the native Ptah command tree is being designed separately before GA.

## Translation model

Implemented Atlas-compatible commands translate Atlas-style flags into Ptah's
native command model and then delegate to native behavior. Unsupported flags
fail clearly instead of being ignored.

| Atlas flag style | Native Ptah concept |
| --- | --- |
| `--url` | `--db-url` |
| `--dir` | `--migrations-dir` |
| `atlas.hcl` env | Project config IR |
| Atlas revision table mode | Ptah revision format and table settings |

## Migration commands

| Atlas-compatible command | Native Ptah command |
| --- | --- |
| `ptah atlas migrate apply` | `ptah migrations up` |
| `ptah atlas migrate down` | Ptah extension path; use native `ptah migrations down` for explicit target rollback recipes until the Atlas-compatible path documents the same runtime contract. |
| `ptah atlas migrate status` | `ptah migrations status` |
| `ptah atlas migrate hash` | `ptah migrations hash` |
| `ptah atlas migrate validate` | `ptah migrations validate` |
| `ptah atlas migrate lint` | `ptah migrations lint` |
| `ptah atlas migrate new` | `ptah migrations create` |
| `ptah atlas migrate set` | `ptah migrations repair` |
| `ptah atlas migrate diff` | Command path registered; runtime behavior is not implemented yet. |
| `ptah atlas migrate import` | Command path registered; runtime behavior is not implemented yet. |

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

## Check before migration

```bash
ptah atlas migrate hash --dir ./migrations
ptah atlas migrate validate --dir ./migrations
ptah atlas migrate status --url "$DATABASE_URL" --dir ./migrations
```

When converting scripts, keep the `atlas` namespace in the Ptah command:

| Do | Do not |
| --- | --- |
| `ptah atlas migrate apply --url "$DATABASE_URL" --dir ./migrations` | `ptah migrate apply --url "$DATABASE_URL" --dir ./migrations` |
| `ptah atlas schema inspect --url "$DATABASE_URL"` | `ptah schema inspect --url "$DATABASE_URL"` |

## Parity expectations

Ptah is not documented as a full Atlas OSS replacement until the external
conformance reports and the comparison gap register support that claim. Use
[Conformance](../../operate/conformance/) for current evidence and
[Comparison](../../reference/comparison/) for tracked product, coverage, and
documentation gaps.
