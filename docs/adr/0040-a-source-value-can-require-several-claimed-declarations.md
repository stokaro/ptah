# ADR 0040: A source value can require several claimed declarations

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Supersedes in part: the one-fragment accounting rule in
[ADR 0037](0037-a-provider-encodes-the-source-it-decodes.md).

Contract: 25.5.

## Context

A YDB table facet containing a key Bloom filter setting and a column family
needs separate Go directives and separate YAML keys. Each declaration is owned
by a different source claim. The decoder combines them into one model value.

The source encoder accounts for each input value exactly once. Repeating its ID
in both fragments is invalid, and a fragment without an ID is invalid too.
Putting both settings under one claim would make the output unreadable by that
claim. A single declaration loses part of the value. This is an accounting
constraint, not an engine-specific spelling problem.

Go annotations also need an explicit declaration of the not-described headers
an owner reads. Encoding such a header without claiming its kind would let an
export weaken coverage without preserving that knowledge on import.

## Decision

### Account for a group, then validate its syntax parts

The two-way feature `source.encode-groups` enables
`EncodedSource.groups`. Each `EncodedSourceGroup` contains nonempty `value_ids`
and an ordered, nonempty list of `EncodedSourcePart` values. A part holds a
`SourceFragment` and its existing placement value. The group writes its values
together; no individual part claims to be their complete representation.

When the feature is negotiated, a successful reply uses groups exclusively.
Without it, the reply uses the flat fragments defined by ADR 0037. A reply never
mixes these representations. Loss-only and empty successful replies need no
group. Every input value appears in exactly one group or one loss, and no
unknown ID appears.

Every part still passes the source claim, syntax, position and placement
validators. A part must claim a model in its group with the same binding and,
when it names a subject, the same normalized owner. Every grouped value must
match at least one part. Distinct column subjects sharing a table binding stay
distinct. A group cannot use one valid part to authorize unrelated syntax.

The host retains group and part order, then writes each part through the
existing format writer. The provider returns syntax data, never a text file.
`EncodedSourceParts` exposes that sequence after validation. It does not decide
whether a provider's spelling preserves the model: encoder/decoder round-trip
tests remain the provider's obligation.

### Preserve coverage from claimed headers

The two-way feature `source.limits` adds `Engine.go_header_kinds`. Each kind
is a unique lower-case identifier that fits one word of a coverage header.
`SourceDocument.limits` may carry those headers for Go annotation input.
An unclaimed kind or a repeated kind/name pair is invalid.

The same feature lets HCL and Go decoding return narrowed knowledge when the
input contains at least one claimed header. All headers must be valid and
claimed. The result keeps the exact enrolled model inventory and uses valid
knowledge states and structured subject or namespace boundaries. Without a
negotiated feature or without headers, the complete authored-scope rule remains.

The provider owns the meaning of a header name and which of its models it
limits. The SDK validates ownership, inventory and knowledge shapes; it cannot
prove an opaque header's semantic interpretation. Host/provider round-trip tests
must prove that known limits retain their meaning and that lost or unrelated
headers do not turn unknown knowledge into absence authority.

When this feature is negotiated, `EncodedSource.limits` follows the format's
header claims and validation. An encode request with narrowed coverage requires
at least one claimed output limit; an empty list cannot carry that knowledge.
The host writes each limit through its format's
coverage-header mechanism and captures it when reading the document. It keeps
the kind, name, reason and provenance. A header cannot upgrade unknown coverage
to complete coverage. Unsupported coverage or an unrepresentable access
constraint must remain a reported loss or a refusal.

Negotiation removes Go header claims when the host does not support them.
Encoding with the flat form remains available when it represents a value
completely; a value needing several parts is a loss for that peer. New Go input
limits require negotiation. The validation of replies to peers without
`source.limits` retains the contract they negotiated.

## Alternatives considered

**Repeat the same value ID in flat fragments.** This removes the exact-once
accounting guarantee and makes accidental duplicate writes indistinguishable
from an intentional multi-part representation. Rejected.

**Put several unrelated declarations inside one fragment.** This breaks the
claim and scope boundary or requires a new wrapper grammar in each format.
Rejected.

**Split the semantic model to fit source syntax.** A model's comparison and
planning boundary should not depend on whether Go, YAML or HCL uses one
declaration. Rejected.

**Treat coverage headers as arbitrary comments.** The decoder cannot establish
which owner understands them, and a round trip can change what absence means.
Rejected.

## Consequences

Providers can preserve a combined model through several claimed declarations
without weakening accounting or source ownership. Hosts must adopt grouped
output and the Go header path explicitly. SDK negotiation and validator tests
cover mixed shapes, missing and repeated IDs, foreign subjects and bindings,
unclaimed syntax and headers, positions and coverage-header round trips.

This contract enables source parity; it does not establish it for any provider.
Each provider must prove canonical model and coverage equality through its
host adapters before claiming a lossless export.
