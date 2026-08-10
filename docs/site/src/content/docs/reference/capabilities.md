---
title: Capabilities
description: The per-dialect capability gates Ptah tracks, plus cross-cutting testing and OCI capabilities.
---

This page tracks the capability gates and cross-cutting capabilities Ptah
supports. What a capability is and how gating works is explained in
[Dialects and capabilities](../../concepts/dialects-and-capabilities/);
per-engine status and operational notes are on the
[Database support matrix](../../databases/support-matrix/).

## What capabilities decide

Capabilities answer questions that a dialect name alone cannot answer:

- Can this target drop constraints with the generic SQL spelling?
  `drop_constraint_generic`
- Can this target guard index drops with `IF EXISTS`?
  `drop_index_if_exists`
- Are CHECK constraints enforced? `check_constraints_enforced`
- Are enums inline column types or standalone custom types?
  `enum_inline_column`, `enum_custom_type`
- Can PostgreSQL-style concurrent indexes be emitted?
  `create_index_concurrently`, `drop_index_concurrently`
- Does the target support roles, RLS, XML, or advisory locks?
  `role_management`, `row_level_security`, `xml_type`, `advisory_locks`
- Which schema objects can this target host?
  `views`, `materialized_views`, `functions`, `triggers`
- Does the target support foreign keys, and how is the referenced key backed?
  `foreign_keys`, `foreign_keys_require_unique_reference`,
  `foreign_keys_require_indexed_reference`,
  `foreign_keys_create_backing_index`

Schema rendering validates foreign keys before emitting any SQL. A malformed
constraint, incompatible column types, an unsupported referential action, or a
target without `foreign_keys` fails the complete render instead of producing
partial DDL or a comment that silently omits referential integrity.

The object-kind keys answer a question a dialect name cannot. PostgreSQL,
CockroachDB, YugabyteDB, and Spanner share one planner and one renderer, so
without them nothing could express that Spanner hosts views but not
materialized views, user-defined functions, or triggers. A refused object kind
is named rather than dropped:
`-- SPANNER: trigger users_touch is not supported by this target; skipped.`
appears in place of the DDL, identically in `ptah schema render` and in the plan
`ptah schema apply` builds, because both pass through the same renderer. A
materialized view presupposes a view and `create_or_replace_trigger`
presupposes `triggers`, so those two requirement edges are validated.

`sequences`, `role_management`, and `row_level_security` answer the same
question and answer it the same way. They used to fail the render instead, and
an error is not something a plan can carry, so the migration planner dropped
roles, grants, and row-level security from the plan before they reached a
visitor, without saying so. Refused objects now use the same named-skip shape
as views, materialized views, functions, and triggers.

For the PostgreSQL family, that refusal path currently applies to Spanner. A
role, grant, sequence, row-level security enablement, or policy is written as a
named `-- SPANNER: ... skipped.` diagnostic instead of being dropped from a plan
in silence. CockroachDB v26.2.5 and YugabyteDB 2026.1 were measured with live
servers and accept those three categories, so their presets enable them.

A refused object is reported again every time the plan is rebuilt, rather than
reported once and then called synced. The skip comment is not a change a
database can absorb. Printed plans keep the diagnostic; the apply execution
path drops comment-only statements before target or dev-database execution.

Exactly one referenced-key policy is enabled for every foreign-key-capable
preset. PostgreSQL, CockroachDB, YugabyteDB, SQLite, SQL Server, and MySQL 8.4+
require a declared candidate key. MySQL before 8.4 and MariaDB accept the
referenced columns as a full leftmost index prefix. Spanner creates and manages
the backing index. A root MySQL 8.4+ connection keeps the conservative
unique-key policy because a pooled session probe cannot describe a later
execution session. `DatabaseConnection.WithSession` refines the policy from
`restrict_fk_on_non_standard_key` on the pinned physical connection, so the
callback plans and executes with one consistent session policy.

The referenced-key policy is only one part of validation. MySQL and MariaDB
require InnoDB tables in Ptah's portable schema path. Their nonunique-key
policy accepts a complete leftmost BTREE prefix, not FULLTEXT, SPATIAL, HASH,
parser-backed, expression, or prefix indexes. The same path rejects generated
FK columns on MariaDB, virtual generated FK columns on MySQL, invalid actions
on MySQL stored generated columns, and mismatched signedness, character sets,
or collations. SQLite accepts standalone candidate keys only when their
collation semantics are represented in the schema IR; otherwise declare the
primary or unique key inline.

Ptah emits `ENGINE=InnoDB` for participating tables when the schema leaves the
engine blank, so a session default cannot silently disable foreign keys.
`SET NULL` requires nullable local columns. Explicit foreign-key names must fit
the target identifier limit: 63 bytes for the PostgreSQL family, 64 characters
for the MySQL family, and 128 characters for SQL Server and Spanner. Generated
names are shortened deterministically before collision checks.

The Go API exposes `capability.DefaultDialects()` for guards and UIs that must
cover every normalized dialect with a default `capability.ForDialect` preset
without maintaining a second list.

## Declarative database testing

`ptah migrations test`, `ptah schema test`, and `migration/dbtest` provide a
workflow capability that composes migration execution, desired-schema
application, seed fixtures, SQL, and assertions against disposable databases.
It is local, MIT-licensed, and requires no account. This workflow is not a
dialect capability flag; supported steps execute through the target's existing
database implementation.

Atlas CE cannot run the corresponding test commands because the testing
framework is outside Atlas's open-source core. See
[Test migrations and schemas](../../testing/migrations-and-schema/) and
[Database test commands](../test-cases/).

## Cross-cutting OCI capability

OCI registry distribution is not a dialect capability key. It is a native Ptah
workflow that applies across supported database targets:

- **Migration artifacts** — push, pull, and direct `up`/`status`/`down` consumption through `oci://`.
- **Desired-schema artifacts** — push/pull canonical `schema.hcl`; compare and drift through `--schema-file oci://...`.
- **Pinning** — unqualified references resolve to `latest`; tags are movable; digest pins are immutable.
- **Authentication** — Docker configuration, `DOCKER_CONFIG`, `credsStore`, and `credHelpers`.
- **Integrity** — optional sum verification before push and before an OCI-backed migration opens the database.
- **Deployment reports** — best-effort redacted referrer after an OCI-backed migration run adds committed revisions, with `--skip-report` opt-out. No-op runs do not publish a report.
- **Referrer publication and listing** — deployment, lint, and plan reports attach to exact source digests. Native Referrers API discovery is preferred; Ptah merges the standard tag-schema fallback with per-attachment durable tags for concurrent Ptah writers. `ptah oci referrers` lists direct descriptor metadata with type and output-format filters; payload download and consumption are not implemented.
- **Atlas compatibility** — native Ptah only; no Atlas Cloud API, `atlas://`, or implemented Atlas-compatible push command.

See [OCI registry artifacts](../../operate/oci-registry/) for the complete
workflow and security boundaries.
