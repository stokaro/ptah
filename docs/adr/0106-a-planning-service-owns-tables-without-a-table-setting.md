# ADR 0106: A planning service owns tables without a table setting

Status: Proposed

Contract: 25.32

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204);
the open contract gaps of [ADR 0102](0102-the-provider-contract-freezes-with-ptah-1-0-and-keeps-major-25.md)

## Context

A planning service declares the model kinds it plans in `ModelPlanning`. A
parent kind is a table-attached model, a setting a table holds, and a service
with a parent kind is handed every table the host creates, alters, rebuilds
or drops, so it can account for the setting (`engine/planning.go`). Its reply
holds one parent receipt for each such table and parent kind. A provider that
plans every statement of its tables uses that path to own their lifecycle.

DuckDB's tables have no setting of their own. The DuckDB provider declares a
desired and an observed model, `github.com/stokaro/ptah-duckdb-plugin/table-lifecycle`,
that no source declares, no read reports and `NormalizeValues` refuses, only
to name it as the parent kind of its table service (`internal/ownership`,
DuckDB contract gap 5). The model is part of the provider's model identities
and of every comparison and planning vocabulary, and holds nothing.

## Decision

### A service states that it owns tables

`planning.table-ownership` adds `ModelPlanning.owns_tables`. The provider and
the engine list the two-way feature. A service that sets it owns the
lifecycle of every table of its engine, as a service with a parent kind does,
without a table setting model:

- the host hands it every table it creates, alters, rebuilds or drops;
- it answers one parent receipt of kind `table` for each, after the receipts
  of its parent kinds, and a refusal names the table with a parent diagnostic
  of that kind;
- it may plan no change, parent or context kind;
- it reads whole tables and is never outlined;
- an engine has at most one such service.

A host that does not list the feature sees no service own tables this way.
A provider whose only claim on its tables is this one cannot serve such a
host, and says so with its minimum contract.

### What the SDK enforces

`ValidateDescription` refuses an owner without the engine's feature, the
engine's feature without the provider's, a second owner on an engine, and an
owner that reads outlines. `ValidatePlannedFeatures` requires the table
receipt for each table with an action, after those of the parent kinds, and
accepts a parent diagnostic of kind `table`. `NegotiatedDescription` clears
the flag for a host that does not list the feature.

## Alternatives considered

**Keep the placeholder model.** It works today. Every provider that owns its
tables would declare a model that holds nothing, and the frozen contract
would keep that as the only way to own a table.

**An engine-level flag.** `Engine.owns_tables` would say the same without
naming a service. The host would then have to guess which service receives
the tables, and receipts would have no service to come from.

**Receipts keyed by the service instead of a kind.** A table receipt with an
empty kind would avoid the word `table`. The host and the SDK pair receipts
with kinds today, and an empty kind reads as a malformed receipt; a fixed
kind keeps the pairing rule.

## Consequences

- The DuckDB provider can drop its `table-lifecycle` model once it adopts the
  feature; its model identities change with that, as any model's removal
  does.
- The host treats a table-owning service as a parent planner for table
  assignment, receipts and refusals.

## Acceptance evidence

- SDK unit tests: `TestTableOwnership_HappyPath`,
  `TestTableOwnershipReceipts_FailurePath`,
  `TestTableOwnershipDescription_FailurePath` and
  `TestTableOwnershipNegotiation_HappyPath` in `provider-sdk/provider`.
- The conformance suite's narrowed host profiles include the feature for every
  provider that offers it.
