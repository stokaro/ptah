---
title: Capabilities
description: Dialect capability summary and links to detailed support tables.
---

Ptah tracks dialect features through capability metadata. Renderers and
migration planners should check capabilities rather than hard-code optimistic
behavior.

High-level dialect coverage:

| Dialect | Status |
| --- | --- |
| PostgreSQL | Primary first-party target. |
| SQLite | Supported for local and lightweight workflows. |
| MySQL / MariaDB | Supported with dialect-specific limitations. |
| SQL Server | Supported subset with dedicated docs. |
| CockroachDB / YugabyteDB | PostgreSQL-compatible paths with capability differences. |
| ClickHouse / Spanner | Explicit capability-limited support. |

## What capabilities decide

Capabilities answer questions that a dialect name alone cannot answer:

| Question | Example capability |
| --- | --- |
| Can this target drop constraints with the generic SQL spelling? | `drop_constraint_generic` |
| Can this target guard index drops with `IF EXISTS`? | `drop_index_if_exists` |
| Are CHECK constraints enforced? | `check_constraints_enforced` |
| Are enums inline column types or standalone custom types? | `enum_inline_column`, `enum_custom_type` |
| Can PostgreSQL-style concurrent indexes be emitted? | `create_index_concurrently` |
| Does the target support roles, RLS, XML, or advisory locks? | `role_management`, `row_level_security`, `xml_type`, `advisory_locks` |

The same parser or planner family can therefore adapt to MySQL versus MariaDB,
PostgreSQL versus CockroachDB/YugabyteDB/Spanner, and version-specific behavior.

Continue with [Dialect notes](../dialect-notes/) for operational differences
between supported database targets.
