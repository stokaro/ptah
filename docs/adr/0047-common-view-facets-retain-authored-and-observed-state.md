# ADR 0047: Common view facets retain authored and observed state

Status: Accepted

Contract: 25.7

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0033](0033-common-objects-reach-a-provider-through-the-common-wire.md)

## Context

A common view has a SQL body and may also have settings owned by a provider.
The host's desired and observed view models both carry typed facets. The common
view wire carries neither. A provider that reports a view through that wire can
preserve its body and comment while losing the other settings during export.

A live YDB view with a Ptah comment and a separate custom attribute demonstrates
the loss: host export retains the body and comment, but the custom attribute is
absent. Keeping the view only as a native object would disconnect its existing
common declaration, comparison and dependency handling.

Replacement also needs the held state. The common view diff retains the desired
view and the previous body. The rendered `ViewCreation` contains only the desired
view and a replacement flag. A provider cannot determine which observed settings
an undeclared setting should retain from those operands.

## Decision

### Negotiate common view facets

The two-way feature `schema.common-view-facets` extends the common-object
contract. The provider and supporting engine list it alongside
`schema.common-objects`. Negotiation removes it for a peer that does not support
it. Facet fields and observed replacement operands are refused unless the
feature was negotiated. A provider that requires these facts raises its minimum
contract version; the host supplies no embedded fallback.

`ObservedView` gains repeated `Envelope facets`. Each envelope must name the
selected provider, an advertised observed facet model, and this exact view as
its owner. Its source and normalized ownership follows the existing structured
identity and normalization rules. A facet for another view, another object kind
or another database cannot attach to this view.

`SQLView` gains `CapturedFeatures features`. The collection uses the existing
normalized desired facet records and coverage vocabulary. It retains authored
settings and their knowledge state through schema validation, common-object
preparation, source conversion, planning and rendering. Ordinary views and
materialized views bind their respective common kinds. The host never encodes
provider attributes in the common `attributes` string list: that list describes
SQL view options such as `SCHEMABINDING`.

The SDK's inspection inventory includes common-view facets in deterministic
view and facet order. Normalization and conversion account for every input
value. A common view is not also exported as a native object to make a missing
facet disappear from the accounting.

### Carry the observed operand for a replacement

`ViewCreation` gains an optional `ObservedView current` operand. It is allowed
only when `replace` is true. The current view must have the exact same bound
engine, common view kind, database, catalog, schema, name and remaining identity
coordinates as the view being replaced. Existing normalization rules establish
any permitted spelling equivalence; a renderer cannot silently rebind it.

An absent operand means the caller has no captured observation. It never means
that the view or any facet is absent. Authored `CREATE OR REPLACE VIEW` can have
no observation; the provider must either perform it with semantics that need no
such evidence or refuse it. A captured current operand is evidence about the
observed state, not permission to overwrite any newer state without the
operation's normal concurrency checks.

The host retains the full current catalog view in its diff and carries it
through the AST and render adapter. It does not reconstruct current state from
the previous SQL body, from the desired facets, or from a live read hidden in an
offline renderer. Pure creation has no current operand.

A provider defines whether an omitted desired facet preserves a held setting
or requests its removal through its facet and coverage semantics. The host does
not merge opaque payloads. Explicit attribute removal and unknown or omitted
coverage remain distinguishable. Replacement must not discard a held attribute
merely because a declaration changes only the SQL body.

### Keep transport and interpretation separate

The host preserves typed values and checks identity and inventory. The provider
interprets the native attributes and plans their mutations. Common SQL body,
comment, dependency and grant handling retain their existing owners.

SDK validation covers every message carrying `SQLView` or `ObservedView`, not
only inspection. Prepared results cannot invent an observed current operand or
return an unnegotiated facet. Refused conversion or rendering yields no partial
successful output.

## Alternatives considered

**Put native attributes in common SQL options.** This conflates a provider's
key/value attributes with SQL `WITH` options and gives them no typed ownership
or coverage. Rejected.

**Export a second native view object.** The same view then has separate writable
identities and independent common and native lifecycles. Rejected.

**Read the current view while rendering.** Offline rendering has no session and
must remain deterministic over captured input. Rejected.

**Infer current settings from the desired view.** Omission is not observation.
The inference would silently remove settings or invent knowledge. Rejected.

## Acceptance

SDK tests cover feature negotiation, observed and desired view facet ownership,
deterministic inspection accounting, unknown models, source and normalized
identity mismatches, and replacement operands without `replace`. A current
operand for another database or view must fail even when its payload is valid.

Host tests preserve desired and observed facets through validation, preparation,
export, comparison, AST lowering and rendering. Replacement carries the actual
current view; authored creation carries none. Coverage and payloads survive
round trips without a second writable representation.

The YDB public path creates a view with a comment and a custom attribute,
exports and reads it back, and replans without changes. A body-only replacement
retains the held custom attribute. An explicit attribute change or removal is
applied and reaches a fixed point. Refused or unknown current state produces no
partial output and no unrequested attribute removal.

This decision follows separate SDK review, release and consumer pins before
host and provider adoption. It does not establish extraction completion or
authorize the embedded YDB switch without the full parity evidence.

## Replacement subject authority

`ViewCreation.current.identity` is the full observed subject. The desired
`SQLView` binds kind, schema, and name; its facets retain authored relative
ownership. The render change must name the current identity exactly, including
database, catalog, path, and overload. A planning step additionally carries an
operand equal to one captured `current_common.views` entry. These checks reject
an invented current observation without treating the authored declaration as
proof of a database binding. A host constructing a render request is responsible
for retaining that exact observation from its inspection and comparison.
