---
title: Public Go API
description: Stable embedder packages and API compatibility guardrails.
---

Ptah is pre-GA, but embedders need a documented import surface. The packages on
this page are the stable embedder API. Packages not listed here are command
packages, examples, fixtures, tests, or implementation details.

## Stable Packages

| Package | Purpose |
| --- | --- |
| `atlascompat` | Stable wrappers for Atlas-compatible schema, SQL, and migration-sum behavior. |
| `config` | Project-level config loading helpers. |
| `config/projectconfig` | Ptah project config IR and Atlas project config mapping. |
| `core/ast` | Typed schema DDL AST nodes. |
| `core/goschema` | Go annotation parser and schema IR. |
| `core/platform` | Dialect and platform constants. |
| `core/platform/capability` | Capability flags for dialect/version behavior. |
| `core/ptaherr` | Typed public errors and sentinel errors. |
| `core/renderer` | Dialect-aware SQL rendering from AST/schema IR. |
| `core/sqlutil` | SQL utility helpers used by public paths. |
| `dbschema` | Live database schema introspection connection layer. |
| `dbschema/types` | Shared database schema types. |
| `migration/generator` | Migration file generation. |
| `migration/lint` | Migration SQL linting rules and findings. |
| `migration/migrator` | Migration providers, revision metadata, dry-run plans, and execution. |
| `migration/planner` | Schema change planning. |
| `migration/risk` | Migration risk classification. |
| `migration/safety` | Destructive-change assessment and safety reports. |
| `migration/schemadiff` | Desired/live schema diffing. |
| `migration/schemadiff/types` | Shared schema-diff types. |
| `migration/seeder` | Seed discovery and execution. |

Import paths use the module prefix:

```go
import "github.com/stokaro/ptah/core/renderer"
```

## Error Contracts

Public failures should use `core/ptaherr` when callers can reasonably branch on
the error:

- annotation and parser failures should support `errors.As` with
  `*ptaherr.ParseError`;
- unsupported dialect failures should support `errors.Is` with
  `ptaherr.ErrUnsupportedDialect`;
- command wrappers should preserve typed errors instead of replacing them with
  string-only errors.

## API Guardrails

CI protects the public API in three layers:

| Check | Purpose |
| --- | --- |
| `scripts/check-public-api.sh` | Fails if a new importable package appears outside the stable list. |
| `scripts/check-public-api-snapshot.sh` | Compares exported symbols with `docs/public_api.snapshot`. |
| `scripts/check-public-api-released.sh` | Compares stable packages against the latest `v0.x` release tag with `apidiff`. |

Any intentional public API change must update the docs and snapshot in the same
reviewed PR. Once release baselines exist, incompatible changes also require an
explicit approval entry.

## Embedding Guidance

Use [Reusable components](../reusable-components/) for task-oriented examples.
Use this page to decide whether a package is supported for embedding. Do not
import `internal/...` packages from another module.
