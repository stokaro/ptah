# ADR 0024: The provider owns table changes through one lifecycle service

Status: Proposed

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

## Context

The YDB provider creates row tables. It does not change them. Its planner refuses
`PlanningTable_ALTER` with "Only native row-table creation is implemented"
(`internal/tablecreate/plan.go`), and its renderer accepts only a native
statement with no parent. The host still plans, compares and renders every
change to an existing table, about 27k lines in the packages listed in the
coverage table of ADR 0023.

Column families are the smallest family to move, and they show four couplings
that any table change has:

- The family rules (`internal/ydbfamily`, 567 lines) read the dropped and the
  declared columns, so a family change depends on column add and drop in the
  same plan.
- The plugin has no family model. Families live inside `TableProperties` as
  `RowTableIntent.column_families` and in `ColumnProperties.family`. The host
  compares TTL, partitioning and families as separate facets.
- Planning parent kinds are exclusive. `TableProperties` and `ColumnProperties`
  are already claimed by the creation service, so a second service cannot claim
  them.
- The plugin creates tables with the native `CreateTable` call and confirms by
  reading back. It has no alter call and no confirmation for one.

## Decision

**One lifecycle service per provider engine owns create, alter and drop of a
table.** The creation service becomes the lifecycle service. It claims the
parent kinds once and handles every `PlanningTable` action. A second service
that wants the same kinds is a conflict the provider description refuses, as
today.

**Each setting is a facet with its own model.** The plugin declares narrow
models for column families, TTL, partitioning, and the other settings the host
compares separately today. It does not compare whole `TableProperties`. A
whole-table comparison would overlap the host's remaining facet comparisons
during the move and would hide which setting changed. A new model is a protocol
and schema addition, taken before the v1 freeze.

**The provider alters through the native API.** A change runs as a native
`AlterTable` call followed by a read-back confirmation, the same shape as
creation. The provider does not emit `ALTER TABLE` text for the host to execute.
The host keeps no YQL rendering for tables.

**The provider claims column add and drop.** The renderer accepts `add_column`
and `drop_column` changes for tables it owns, so ordering between columns and
settings (family after add, before drop) is expressed in one plan graph with one
owner.

**Each pull request moves one facet into the plugin. The host code is deleted
after the default flips.** A selected provider replaces the whole runtime, and
the built-in registry refuses to register a provider under the name `ydb`. The
host's YDB planner, comparer and renderer therefore stay reachable for every
user who has not configured the plugin, until the default engine for `ydb://`
URLs is the plugin. Deleting one family earlier would break the built-in path
for that family. The order is: the plugin gains a facet, the host accepts the
matching change for a provider that owns the table lifecycle, the default flips
to the plugin, and then the host packages go in dependency order. The host's
source readers (`ydbsource`, `yqlparse`) and the declared-family refusal in
`engine/builtin/renderer.go` read the family rules, so `internal/ydbfamily` goes
with the sources step.

## Alternatives considered

**Compare whole `TableProperties` in the provider.** It needs no new model and
the first pull request is smaller. The host and the provider then both compare
TTL and partitioning until both move, and a diff cannot say which setting
changed. It trades one protocol addition for a long period of overlap.

**Emit `ALTER TABLE` text from the provider.** The host executes it and the
change fits the existing statement path. It splits the contract: creation is
native and alteration is text, a failure has no read-back, and the provider
cannot confirm the change landed. It also keeps a YQL shape in the host's
expectations.

**A second service for alteration.** Each service stays small. The exclusive
parent kinds make it impossible without splitting `TableProperties`, and the
column ordering the family depends on crosses the two services.

**Leave column add and drop in the host for provider tables.** The first pull
request is smaller. The plan graph then has two owners for one table, and
ordering between them becomes a host rule about YDB.

## Consequences

The first pull request is larger than a family move alone: it needs the
lifecycle service shape, the alter operation model, the alter call with
confirmation, and column add and drop in the renderer. Later families add a
model, a comparison and a plan branch each.

The protocol grows one facet model per family before v1. The conformance suite
covers each.

Host deletion happens after the default flips, in dependency order, and some
host code (`ydbfamily` and its source readers) lives until the sources step. The
coverage table in the working notes tracks which files remain.

## Amendments

- [ADR 0029](0029-column-tables-need-no-protocol-change-and-the-address-stays-provider-owned.md)
  adds column storage as a facet of this lifecycle service: creation by the native
  call with read-back, and comparison where only the tiered TTL changes in place.
- [ADR 0030](0030-ptah-compat-resolves-providers-through-the-native-pipeline.md)
  tracks the table-rebuild policy input as a flip blocker, so the lifecycle service
  must accept that input before the embedded planner is deleted.
