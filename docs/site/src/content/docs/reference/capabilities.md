---
title: Capabilities
description: Dialect capability summary and links to detailed support tables.
---

Ptah tracks dialect features through capability metadata. Renderers and migration planners should check capabilities rather than hard-code optimistic behavior.

High-level dialect coverage:

| Dialect | Status |
| --- | --- |
| PostgreSQL | Primary first-party target. |
| SQLite | Supported for local and lightweight workflows. |
| MySQL / MariaDB | Supported with dialect-specific limitations. |
| SQL Server | Supported subset with dedicated docs. |
| CockroachDB / YugabyteDB | PostgreSQL-compatible paths with capability differences. |
| ClickHouse / Spanner | Explicit capability-limited support. |

References:

- [Detailed capability matrix](https://github.com/stokaro/ptah/blob/master/docs/capabilities.md)
- [SQLite notes](https://github.com/stokaro/ptah/blob/master/docs/sqlite.md)
- [SQL Server notes](https://github.com/stokaro/ptah/blob/master/docs/sqlserver.md)
