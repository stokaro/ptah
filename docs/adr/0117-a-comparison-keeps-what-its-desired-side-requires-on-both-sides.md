# ADR 0117: A comparison keeps what its desired side requires on both sides

Status: Proposed

Contract: none (host only)

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)
(register G227)

Amends: [ADR 0039](0039-a-provider-declares-how-a-scope-names-its-objects-and-which-grants-name-them.md)

## Context

A selection can keep an object no selector names, because a kept object
requires it: a YDB resource pool classifier keeps the pool it routes to, and so
does a streaming query (`keeps_required`, ADR 0039; the built-in YDB engine's
`selectext` service before it). ADR 0039 keeps such an object "when the side
holds it". A comparison projects each of its two sides alone, so each side kept
what its own kept objects require.

Two sides of one comparison then disagree about the same object. Measured on
YDB 26.2.1.14 with resource pools on, the YDB provider at plugin master
535f8bd7, and host master e31ce4f63, with `schema diff --include
<classifier>[type=resource_pool_classifier]`:

- The database holds the pool, and the declaration declares the pool and a new
  classifier. The declaration keeps the pool for its classifier, the database
  read has no classifier and keeps nothing, and the plan is `CREATE RESOURCE
  POOL` of the held pool and `CREATE RESOURCE POOL CLASSIFIER`. `schema apply`
  stops before the first statement (`NOT_STARTED`), and the classifier is never
  created.
- The database holds two pools and a classifier on the first, and the
  declaration moves the classifier to the second. The read keeps the first
  pool, the declaration keeps the second, and the plan creates the second pool
  again beside `ALTER RESOURCE POOL CLASSIFIER`.
- The same holds for `schema diff` between two databases.

A source declares the pools and classifiers it manages, not every one of the
database, so on YDB a pool kept on the read alone is not dropped. An owner whose
sources describe a kind completely would see the mirror case: a classifier only
the database keeps would bring its pool into the plan as a removal no selector
asked for. The built-in YDB engine had the same rule and the same defect.

## Decision

A comparison decides its requirements once, from its desired side, and both
sides keep them.

The first projection of each side reports the objects it kept only for a
requirement, which the side holds (`atlasfilter.SelectionReport.Required`).
When either side kept such an object, the comparison projects both sides again
with the desired side's list (`atlasfilter.Scope.Requirements`,
`DesiredRequirements`). Each side keeps those objects where it holds them, and
keeps nothing for a requirement of an object only the current side keeps. An
exclusion stays authoritative on both sides.

A projection of one state, such as `schema inspect`, keeps what its own kept
objects require.

`schema apply`, `schema diff` and the read-only verbs that take
`atlasschema.LiveSelection` are the comparisons. The desired side is the
declaration, and for `schema diff` the `--to` state.

## Alternatives considered

**Keep the union of both sides' requirements on both sides.** The first two
cases converge as well. A classifier only the database keeps would still bring
its pool into the comparison, so an owner whose sources describe a kind
completely would plan the pool's removal, and a classifier moved to another
pool would plan a change of its old pool whose settings differ. Neither is
something the selector asked for.

**Drop the requirement, as ADR 0039 does for any kind outside
`keeps_required`.** The YDB provider took this path for external tables and
data sources (G225). For pools it would refuse every include of a classifier
alone: the provider refuses a declared classifier whose pool the declaration
does not declare, because YDB runs such a classifier's queries in the pool
`default` without a word (stokaro/ptah-ydb-plugin#360).

**Compute the requirements in the provider.** The provider sees one value at a
time and never the two sides of a comparison; the scope is the host's.

## Consequences

- An include of a declared classifier beside a held pool plans the classifier
  alone, and a classifier moved to a held pool plans its change alone.
- A comparison that kept an object for a requirement projects its sides twice,
  as a comparison that matched a non-extension resource already does.
- The rule covers owner requirements. A common type a kept column names (an
  enum, a domain) is still kept per side.

## Acceptance evidence

- `TestScopeDatabaseSelectionReport_ListsWhatARequirementKept` and
  `TestScopeDatabase_KeepsTheRequirementsAComparisonDecided` in
  `internal/atlasfilter`.
- `TestLiveSelection_ApplyKeepsARequiredObjectOnBothSides` in
  `internal/atlasschema`.
- `TestProviderScopeSelectionsKeepARequiredPoolOnBothSides` in
  `integration/providercli`, through the scope-selections fixture provider.
- With the YDB provider on 26.2.1.14: the first case plans `CREATE RESOURCE POOL
  CLASSIFIER` alone and `schema apply` creates it, the second plans `ALTER
  RESOURCE POOL CLASSIFIER` alone, and a classifier and a pool neither held
  are both created.
