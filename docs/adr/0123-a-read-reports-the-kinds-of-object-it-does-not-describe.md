# ADR 0123: A read reports the kinds of object it does not describe

Status: Proposed

Contract: 25.41

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)
(register S16)

## Context

An engine can hold kinds of object that its provider does not model. A
Spanner change stream is one: it has a lifecycle of its own, and the Spanner
provider reads none. A read that leaves such a kind out must say so, or its
silence reads as the absence of every object of the kind, and a plan made
against a document from that read can remove what it never saw.

Ptah's built-in Spanner engine said so. Its reader recorded the kind for the
target, whatever the database held, so every document made from a read
carried the line:

```text
// ptah:not-described change_stream reason=unsupported provenance=derived-from-target
```

`schema inspect --format json` listed the same record under `NotDescribed`.
The kind came from the engine's registration in the host process
(`engine.Provider.CoverageKinds`), which adds it to the vocabulary that
documents may name, so the host reads the line back.

An executable provider has no way to do this. Measured with a change stream in
the database, the Spanner provider's documents carry no `change_stream` line,
and nothing in the contract would let them:

- Model coverage accepts only a value model the engine declares, so a kind
  without a model has no coverage record.
- `Schema.dispositions` exists on the wire, and the host refuses any read that
  carries one.
- The coverage of common objects names a closed set of common kinds.
- An unread table is a table, and the host reports it only as a note on the
  diagnostics stream.
- The host's vocabulary holds only the common kinds and the kinds of engines
  registered in its process, so a document line that names a provider's kind
  is refused when the host reads it back.

## Decision

`schema.not-described` adds two fields: `Engine.not_described_kinds` in the
description and `Schema.not_described` in a read.

An engine that lists the feature declares, in `Engine.not_described_kinds`,
the kinds of object its reads record and do not describe. Each kind is a
lowercase token, such as `change_stream`, and the engine declares it once. The
host adds the kinds to the vocabulary its documents may name. A kind belongs
to the engine: the host refuses a kind that is one of its common kinds or
another provider's.

A read reports what it does not describe in `Schema.not_described`, as
`SchemaCoverageLimit` records, the message the host already sends in
`ValidateSchemaRequest.not_described`. A record names a kind the engine
declares and, optionally, one object of it; without a name it covers the
whole kind. It states why the read does not describe the object and how the
read learned that, in the host's spelling. The reasons a read gives are
`not-inspected`, `outside-scope`, `unsupported` and `unresolved`. The
provenances are `observed`, `derived-from-target`, `derived-from-fact`,
`inferred` and `unavailable`. The host checks each record against the
engine's declared kinds and keeps it as one of the read's not-described
records, so a document made from the read says that it does not describe the
object, and a plan does not read its absence as a removal.

The feature is two-way. The provider lists it, and so does each engine that
declares kinds. An engine that lists it declares at least one kind. A host
that does not list it never receives the kinds or a record. The SDK refuses
these descriptions and replies:

- kinds on an engine that does not list the feature, the feature on an engine
  without the provider's, and an engine that lists it with no kind;
- a kind that is not a lowercase token, or one declared twice;
- records in a read whose session did not negotiate the feature;
- a record of a kind the engine does not declare, a name that is blank, holds
  a control character or is not UTF-8, a reason or a provenance outside the
  lists above, and the same kind and name recorded twice.

## Alternatives considered

**Accept `Schema.dispositions` instead of a new field.** A disposition names a
field of an object a read describes, by its path, and says how far the read
interpreted it: `MODELED`, `NORMALIZED`, `UNSUPPORTED` or `NOT_INSPECTED`. The
SDK treats the last two as an incomplete read (`field ... has no complete
interpretation`). A not-described record is the opposite: a complete read
that declares what it leaves out. Taking dispositions for it would give the
two last states a second meaning, need a convention for a whole kind that a
`Reference` with a field path does not have, and still lack the provenance the
host's records carry. `SchemaCoverageLimit` carries exactly the host's record,
kind, name, reason and provenance, and the host already writes it in the other
direction. The new field is the better fit.

**Declare a value model for each kind and report it as `UNINSPECTED`.** Model
coverage then says that the read did not inspect the kind. The kind would be a
model in every other respect: a definition, codecs for both representations,
and a place in every export's feature coverage and in the Go source a read
writes, which the built-in engine never had. A model is a promise of support,
and change streams are outside what the Spanner provider supports.

**Report each change stream as an unread table.** An unread table is reported
as a table, on the diagnostics stream only, and only when a stream exists, so
no document would say that the read leaves change streams out.

## Consequences

- The Spanner provider can declare `change_stream` and record it, unsupported
  and derived from the target, on every read, so its documents carry the line
  the built-in engine wrote.
- A provider that does not list the feature reads as before.
- Another engine can record its own kinds under the feature with no new minor.
- Format limits, the kinds a document format has no spelling for, stay with
  engines registered in the host process. An executable provider that needs
  one needs another addition.

## Acceptance evidence

- SDK: `TestNotDescribed_HappyPath`, `TestNotDescribedDescription_FailurePath`
  and `TestNotDescribedInspection_FailurePath` in `provider-sdk/provider`.
- Host adoption: the host lists the feature, adds an engine's kinds to its
  vocabulary and keeps a read's records, and a fixture provider whose read
  records a kind has the line in the header of an HCL export, which the host
  reads back.
- Spanner adoption: the header of every read's HCL export says that it does
  not describe change streams, with a change stream in the database and
  without one.
