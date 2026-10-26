# ADR 0050: Statement style reaches feature owners

Status: Proposed

Contract: 25.8

Deciders: Ptah maintainers

Issue: [stokaro/ptah#5000](https://github.com/stokaro/ptah/issues/5000)

Relates to: [ADR 0035](0035-the-operators-planning-policy-reaches-the-provider.md),
[ADR 0030](0030-ptah-compat-resolves-providers-through-the-native-pipeline.md)

## Context

`ptah-compat migrate diff` selects `planext.StatementStyleExact`. The common
PostgreSQL planner then omits an existence guard and `CASCADE` unless its plan
relies on them. A feature owner receives `featureplan.Request` without that
style. The pgtype owner still renders a removed domain as `DROP DOMAIN IF EXISTS
... CASCADE`.

Issue #5000 measured the consequence on PostgreSQL 18.6. A domain removed from
the declaration still typed a column in an undeclared `reporting` schema. The
generated migration applied successfully and silently removed that column and
its data. Without `CASCADE`, PostgreSQL refused the drop and named the dependent
column. The pgextension owner also keeps `IF EXISTS` on a removed extension;
its drop does not use `CASCADE`.

The current `planning.policy` feature carries other plan choices to providers,
but a provider built before this decision can negotiate that feature and ignore
an unknown protobuf field. Sending exact style under that feature alone would
repeat the unsafe behavior without an error. The SDK currently speaks contract
25.7, so this addition belongs to the next minor contract.

## Decision

### Carry one style to every feature planner

`featureplan.Policy` gains a typed `Statements` field, filled from the same
`planext.Options.Statements` value that the common planner uses. `planext`
already imports `featureplan`, so the boundary maps the two values explicitly
instead of introducing an import cycle or a public compatibility alias. The
zero value remains annotated style. `PlanningPolicy` gains a `statement_style` enum
with `ANNOTATED = 0` and `EXACT = 1`; an unknown value is refused at both
transport boundaries. Declaration rendering supplies annotated style because
it does not generate an exact migration file.

`PlanFeaturesRequest.policy` remains the wire location. The separate two-way
feature `planning.statement-style` proves that both the provider and selected
engine understand the new field. It requires `planning.policy`. A provider and
host advertise it independently; the host never infers support from contract
minor or from the presence of `planning.policy`.

When an exact plan invokes a provider planning service that lacks the feature,
the host refuses before dispatch. It does not substitute annotated style or
post-process rendered SQL. A service may emit an operation from a change,
parent action, or object-context assessment, so the host cannot assume an
invoked service will produce no owner statement. An annotated plan remains
usable with an older provider.

### Owners honor the style in their own operations

The pgtype owner gives `pgtype.Drop` an explicit `IfExists` field, required in
its codec shape so an older encoded drop cannot acquire new meaning. A type or
domain removed for good is emitted with `IfExists: true`
and `Cascade: true` in annotated style, preserving its existing spelling. In
exact style both are false. A type rebuilt in the same plan already omits
`CASCADE`; its drop also omits `IF EXISTS` in exact style. The server then
refuses a dependent object outside the declaration before it loses data.

The pgextension owner omits `IF EXISTS` from an exact-style removal. The
pgpolicy owner already renders its policy drops without the guard. Every owner
keeps responsibility for its typed operation and renderer; the host does not
rewrite an owner's SQL.

`StatementStyleExact` documentation and the provider host's owner-statement
comment must reflect that owner operations honor the style after negotiation.

## Alternatives considered

**Remove `CASCADE` from every pgtype drop.** This makes the dangerous example
fail, but changes Ptah's annotated style and leaves the owner unable to honor
the selected style. Rejected.

**Add a field only to `PlanningPolicy`.** An older provider already negotiating
`planning.policy` can ignore it and return the same dangerous drop. Rejected;
the new two-way feature is the compatibility and safety boundary.

**Rewrite an owner's SQL in the host.** The host would have to parse each
owner's statement and guess which guards and dependencies are intentional.
That duplicates owner knowledge and cannot make an undeclared dependent safe.
Rejected.

## Consequences

- Contract 25.8 adds a field and a two-way feature. Exact planning with an
  older provider may refuse until that provider is upgraded.
- `pgtype.Drop` changes its typed codec shape. Existing annotated plans retain
  their statement spelling.
- A migration that would remove an undeclared dependent now fails at the
  PostgreSQL drop, leaving that dependent and its data in place.

## Acceptance evidence

- SDK and host tests refuse unknown styles, an exact request without the new
  negotiated feature, and a provider description that claims it without
  `planning.policy`. An annotated request still reaches an older provider.
- In-process and executable pgtype providers emit unguarded, non-cascading
  exact drops for removed domains and composite types; annotated output stays
  pinned. Pgextension drops use the selected guard.
- A live migration removes a declared domain that an undeclared column uses.
  The exact up file fails with PostgreSQL `2BP01`, and the column and its row
  value remain. The corresponding annotated control retains its spelling.
- Generated SDK files, public contract documentation, and contract-version
  tests describe the new field and feature in the same change.
