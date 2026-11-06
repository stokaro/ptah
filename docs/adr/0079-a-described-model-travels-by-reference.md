# ADR 0079: A described model travels by reference, and a schema fits one message

Status: Proposed

Contract: 25.26

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0022](0022-provider-values-are-resolved-in-semantic-batches.md),
[ADR 0075](0075-a-provider-names-the-identities-its-models-had-before.md)

## Context

A provider describes each of its models once, in `Engine.value_models`: the
kind, the representation, the schema version and the definition, with the
definition's digest. A coverage record names the model it covers with a
`ValueModel` message, and the SDK accepts the record only when that message
equals the described one, definition included. The host captures a coverage
for each state of each table it sends, desired and current, objects and
facets, so every request that carries tables repeats every model's
definition once per table and state. Each message between the host and a
provider is limited to 4 MiB.

Measured with the host at 6959d7f70 and the YDB provider at e271ac77, offline
with `ptah schema validate`: the provider's description is 933,042 bytes, of
which 860,600 are definitions. A validation request of 15 trivial tables is
4,246,104 bytes, 3,855,270 of them definitions in the tables' coverage, about
265 KB per table, and the host refuses to send it (`ResourceExhausted`).
Eleven tables pass. Live on YDB 25.1.4.7 and 26.2.1.14 every planning command
stops the same way at that size, in an empty dev realm too. The built-in YDB
engine at 74de0b65b had no such bound: it validates 500 tables in about a
second and applies them in 16 seconds.

The definitions are most of the bytes, not all of them. With every
definition replaced by a reference, a table preparation request of 447
existing tables and 53 new ones is 10,503,343 bytes: about 23 KB per existing
table, mostly the read's knowledge of each model, which every table's current
state repeats, and the facet values the read observed. A synchronized plan of
a directory passes 4 MiB at about 180 tables on that reckoning.

## Decision

### A described model travels by reference

`models.references` is a two-way feature of the provider alone. Where both
sides list it, the sender replaces every `ValueModel` outside `Describe` that
equals a model of the negotiated description by its reference: a
`ValueModel` with only its `kind`, `representation`, `schema_version` and
`definition_sha256`. A model the description does not declare, such as one a
captured document names under another definition, and one two engines declare
differently, travels whole. The receiver replaces each reference with the
described model before a service or a validator reads the message, so a
refusal of an unknown or changed model reads that model as it was written.

The SDK does both ends. The client sends references and resolves the
references of each reply; the server resolves the references of each request
and sends references in each reply. No host adapter and no provider service
sees a reference. A reference keeps the digest: a receiver that does not hold
the definition the sender named refuses the message rather than read the
claim under another definition.

### A message may be 64 MiB

`transport.large-messages` is a two-way feature of the provider alone. Where
both sides list it, a request and a reply may be 64 MiB
(`runtime.LargeMessageBytes`); otherwise the bound stays 4 MiB
(`runtime.MaxMessageBytes`). The SDK server receives and sends up to 64 MiB
with any host, since a host that does not list the feature sends no more than
4 MiB and receives no more. The SDK client sends more than 4 MiB only to a
provider that negotiated the feature, so an older provider meets the same
refusal it meets today.

### What the SDK enforces

A receiver refuses a reference that carries more than its identity, one that
names no model of the negotiated description, and one that names a model two
engines declare differently; the server answers `InvalidArgument` and the
client reports the reply as malformed. Without `models.references` nothing is
resolved, and the validation of the message refuses a reference as a model
the description does not declare. A host that does not list a feature
receives neither.

## Alternatives considered

**Name the provider's native grammar by its digest in each definition**
(stokaro/ptah-ydb-plugin#274). It shrinks the definitions to about a third,
87 KB per table, and the bound moves to about 47 tables: each table still
repeats every definition. With references a definition's size no longer
counts per table.

**Give each table's state only the kinds of its placement.** The host would
put the object models in a table's objects state and the facet models in its
facets state. It halves the records and leaves the bound at a few hundred
existing tables.

**Name a model by its kind alone, or by its position in the description.** A
shorter reference saves half of what the digest costs and loses the check
that both sides mean the same definition.

**Split a large request.** A plan orders the tables of a schema among each
other, by the keys and the dependencies between them, and a validation reads
the dependencies of the whole schema, so a part of a request cannot be
planned or validated alone.

**Raise the bound alone.** At 265 KB per table, 64 MiB holds about 250 tables,
and every copy of a request holds every definition again.

## Consequences

- A schema of thousands of tables reaches a provider that lists both features
  in one request. Measured with both features: an offline validation of 5,000
  tables is one request of 21,914,700 bytes, about 4.4 KB per table; live on
  YDB 25.1.4.7 and 26.2.1.14 a directory of 500 tables plans as synced, its
  table preparation and planning requests about 11 MB each. A planning
  request carries about 23 KB per existing table, so 64 MiB holds a synced
  plan of about 2,900 tables, and a migration's reverse plan, which carries
  both states, about 1,900.
- A copy of a request the SDK makes holds each model once: the client's copy
  before it sends a request and the server's copy for its service share every
  model the original shared, where `proto.Clone` copied the definition once
  for every table. Validation checks each definition of a description once
  per process rather than once per table.
- A peer of an earlier contract exchanges whole models in messages of at most
  4 MiB, as before.
- What is left per table is the read's knowledge and its facet values. A
  later addition can send a repeated coverage once; it needs a field.

## Acceptance evidence

- SDK unit tests: `TestReferenceModels_HappyPath`,
  `TestResolveModelReferences_HappyPath`,
  `TestResolveModelReferences_FailurePath`,
  `TestModelReferenceWithoutResolution_FailurePath` and
  `TestCloneSharingModels_HappyPath` in `provider-sdk/provider`;
  `TestModelReferences_HappyPath`, `TestServerModelReferences_FailurePath`,
  `TestClientModelReferences_FailurePath`, `TestLargeMessages_HappyPath` and
  `TestLargeMessages_FailurePath` in `provider-sdk/runtime`.
- The host lists both features.
- The YDB provider validates a schema of 500 tables in one request, and
  refuses it at 4 MiB to a host that lists neither feature.
