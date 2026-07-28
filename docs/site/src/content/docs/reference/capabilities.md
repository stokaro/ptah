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

| Question | Example capability |
| --- | --- |
| Can this target drop constraints with the generic SQL spelling? | `drop_constraint_generic` |
| Can this target guard index drops with `IF EXISTS`? | `drop_index_if_exists` |
| Are CHECK constraints enforced? | `check_constraints_enforced` |
| Are enums inline column types or standalone custom types? | `enum_inline_column`, `enum_custom_type` |
| Can PostgreSQL-style concurrent indexes be emitted? | `create_index_concurrently` |
| Does the target support roles, RLS, XML, or advisory locks? | `role_management`, `row_level_security`, `xml_type`, `advisory_locks` |

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

| Area | Support |
| --- | --- |
| Migration artifacts | Push, pull, and direct `up`/`status`/`down` consumption through `oci://`. |
| Desired-schema artifacts | Push/pull canonical `schema.hcl`; compare and drift through `--schema-file oci://...`. |
| Pinning | Unqualified references resolve to `latest`; tags are movable; digest pins are immutable. |
| Authentication | Docker configuration, `DOCKER_CONFIG`, `credsStore`, and `credHelpers`. |
| Integrity | Optional sum verification before push and before an OCI-backed migration opens the database. |
| Deployment reports | Best-effort redacted referrer after an OCI-backed migration run adds committed revisions, with `--skip-report` opt-out. No-op runs do not publish a report. |
| Referrer publication and listing | Deployment, lint, and plan reports attach to exact source digests. Native Referrers API discovery is preferred; Ptah merges the standard tag-schema fallback with per-attachment durable tags for concurrent Ptah writers. `ptah oci referrers` lists direct descriptor metadata with type and output-format filters; payload download and consumption are not implemented. |
| Atlas compatibility | Native Ptah only; no Atlas Cloud API, `atlas://`, or implemented Atlas-compatible push command. |

See [OCI registry artifacts](../../workflows/oci-registry/) for the complete
workflow and security boundaries.
