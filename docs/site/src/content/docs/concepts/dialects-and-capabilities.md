---
title: Dialects and capabilities
description: Why Ptah separates the dialect it renders SQL for from the capabilities a concrete database target accepts.
---

Ptah keeps two ideas apart that database tools often blur. A **dialect** is a
SQL rendering flavor — the family of syntax Ptah generates, such as
`postgres` or `mariadb`. A **database engine** is the product you connect to.
Because engines that share a syntax family still accept different operations
— and versions of one engine differ too — the dialect name alone cannot
answer "is this operation valid on my target?". That question is answered by
a **capability**: a per-target feature gate such as `create_index_concurrently`
or `row_level_security`.

## How Ptah models it

Dialect names normalize first: `postgresql` means `postgres`, `sqlite3` means
`sqlite`, `mssql` means `sqlserver`, and so on. Every accepted spelling of an
engine produces byte-identical DDL; a test derives its spelling list from the
normalization function itself, so a new spelling cannot be added without being
covered.

Each normalized dialect maps
to an implementation family, and several engines deliberately share one —
MySQL and MariaDB share a planner family, and CockroachDB, YugabyteDB, and
Spanner ride the PostgreSQL family. What distinguishes the members is their
**capability set**: a validated map of known capability keys, with presets per
engine and version line (PostgreSQL 12–13, 14–16, and 17+ are three different
presets). Unknown keys and contradictory sets are rejected outright, so a
typo in a capability name fails fast instead of silently changing plans.

Which set applies depends on how a command runs:

- **Live connections** read the server's version banner and select the
  matching preset — a MariaDB server behind a `mysql://` URL still resolves
  to the MariaDB preset, and a CockroachDB banner on a PostgreSQL wire
  resolves to the CockroachDB preset. That resolved set travels with the
  connection through planning, rendering, and safety assessment.
- **Offline commands** (rendering to stdout, diffing files) have no server to
  ask, so they use the dialect's current-version default preset.

Capabilities act at two layers. The planner records *intent* on planned
operations according to its target's capabilities, and the renderer checks
that intent against *its own* capability set before emitting SQL — so a
guard or clause the concrete target would reject is stripped or re-spelled
rather than shipped.

## Consequences

- **Shared families adapt instead of forking.** One PostgreSQL-family
  implementation serves four engines honestly, because each engine's preset
  removes what it does not support.
- **Unsupported operations fail loudly.** Where an operation has no valid
  spelling for the target, Ptah emits an explicit error or, for explicitly
  documented advisory operations, a loud `WARNING` comment. Schema rendering
  never replaces a declared foreign key with a comment.
- **Foreign-key validity is target-specific.** Capability presets distinguish
  candidate-key targets, MySQL-family indexed-left-prefix targets, and
  Spanner's engine-managed backing indexes. Root MySQL 8.4+ connections retain
  the conservative unique-key policy; a pinned `WithSession` callback refines
  it from `restrict_fk_on_non_standard_key` on the same physical connection
  used for execution. Ptah also
  validates column types, name namespaces, and engine-specific index/storage
  restrictions before emitting SQL.
- **Version upgrades can change plans.** Moving a server across a preset
  boundary (for example PostgreSQL 13 to 14) legitimately changes generated
  SQL, such as trigger replacement switching to `CREATE OR REPLACE TRIGGER`.
- **Embedders can pin.** The Go API accepts explicit capability sets, so
  tests and CI can plan against a fixed server profile regardless of what
  they connect to — see [Reusable components](../../extend/components/).

## Where it appears

- Per-engine status and operational notes: [Database support matrix](../../databases/support-matrix/).
- The capability keys and cross-cutting capabilities: [Capabilities](../../reference/capabilities/).
- Engine pages with capability-driven behavior: [PostgreSQL](../../databases/postgresql/), [SQLite](../../databases/sqlite/), [SQL Server](../../databases/sqlserver/).
