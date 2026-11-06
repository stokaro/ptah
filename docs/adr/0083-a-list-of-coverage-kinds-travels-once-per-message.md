# ADR 0083: A list of coverage kinds travels once per message

Status: Proposed

Contract: 25.27

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0079](0079-a-described-model-travels-by-reference.md)

## Context

Each state of a captured table carries a `ComparisonCoverage`: one
`ComparisonKindCoverage` for each model the engine declares for the state's
representation, naming the model and the sources of its knowledge, and the
subject records of what the read or the declaration knows of particular
objects. ADR 0079 sends each model by reference, so a kind is about 130 bytes
on the wire; the list of kinds is still repeated for every state of every
table, and both sides still copy, walk and check it there.

Measured on the requests of 500 tables through the YDB provider, which
declares 16 models for each representation (remote-dev, YDB 25.1.4.7, host
#5464 and #5471, SDK #5472, provider stokaro/ptah-ydb-plugin#291 and #293):

| request | bytes | coverages | distinct kind lists | bytes of kind lists |
| --- | --- | --- | --- | --- |
| planning, 500 new tables | 2,280,384 | 1,003 | 4 | 2,139,284 |
| creation projection, 500 new tables | 2,262,848 | 1,000 | 1 | 2,134,000 |
| preparation of a synced plan, 500 tables | 11,299,082 | 2,000 | 2 | 8,402,000 |

The lists are nine tenths of a planning request and three quarters of the
preparation of a synced plan, and they repeat one or two lists. Whole
coverages repeat less: the 2,000 coverages of the synced plan's preparation
hold 501 distinct ones, since a read's subject records name each table.

## Decision

### A list travels once, by key

`transport.shared-coverage` is a two-way feature of the provider alone. Where
both sides list it, the sender gives each list of kinds that more than one
coverage of a message carries to the first such coverage, with a nonzero
`ComparisonCoverage.kinds_key`, and gives every other coverage of that list
`kinds_of`, the key, in place of the kinds. Two lists are one list when they
hold, in order, kinds of the same value model messages with equal sources,
so sharing never merges kinds that differ. A coverage keeps its subject
records.

### The SDK shares and resolves

The SDK does both ends, before references are sent and after they are
resolved: the client shares the lists of each request and resolves those of
each reply, and the server resolves those of each request and shares those
of each reply. A receiver gives each coverage that names a key the kinds of
the coverage that defines it, as the same kind messages, and clears every
key, so no host adapter and no provider service sees one. The coverages that
repeated a list then hold one message for each kind, and the SDK keeps it so:
`provider.CloneSharingModels` copies each kind message once, as it copies
each model once.

A validation checks each kind message once in each context: the coverages of
a request's tables hold the same messages after resolution, so a request of
500 tables checks its 16 kinds of each representation once, not once per
table.

### What the SDK enforces

A receiver refuses a key two coverages define, a key defined by a coverage
without kinds, a coverage that carries both kinds and `kinds_of`, and a
`kinds_of` that names no key of the message; the server answers
`InvalidArgument` and the client reports the reply as malformed. Without the
feature nothing is resolved, and validation refuses a coverage that carries
either field.

## Alternatives considered

**A field of every request for its shared lists.** Each request type that
carries tables would gain a list of coverages, and each state an index into
it. The lists live where the coverages are, in requests and replies of more
than ten types; a key on the coverage reaches all of them with one field
pair and one walk.

**Coverage of the request's kinds, once per request.** The host would send
the kinds once for the whole request, and a state would say nothing of them.
That changes what a state means: a state that covers other kinds, as a
table the read leaves partly unread does, would need a second form, and
every service that reads a state would have to look for its coverage
elsewhere.

**Share whole coverages.** Most coverages of a read differ in their subject
records, 501 distinct ones among 2,000 in the synced plan, so whole
coverages travel almost as often as before.

**Recognize a list by its content.** Comparing the bytes of each list finds
equal lists the host built from different messages, but a list holds the
full definitions where references are not negotiated, and marshaling a
list of 16 definitions for each of 1,000 coverages costs more than it saves.
The host names one model with one message, so the messages identify a list.

## Consequences

- A request of 500 tables carries each list of kinds once. Measured on the
  same setup with this feature, ADR 0081 and the host and provider that list
  both: the creation projection of 500 new tables is 130,038 bytes
  (2,262,848 before), the planning request of the row tables' owner 140,682
  (2,273,991), and the preparation of a synced plan 2,905,040 (11,299,082).
- A service reads coverages that share kind messages. A change a service
  makes to one kind of one table's coverage is a change to every coverage
  that held the list, as a change to a model already was.
- A peer of an earlier contract exchanges every list in full, as before.

## Acceptance evidence

- SDK unit tests: `TestShareCoverage_HappyPath`,
  `TestResolveSharedCoverage_HappyPath`,
  `TestResolveSharedCoverage_FailurePath` and
  `TestSharedCoverageWithoutResolution_FailurePath` in
  `provider-sdk/provider`; `TestCoverageKindsAreCheckedOncePerKindMessage`
  (white-box) there; `TestSharedCoverage_HappyPath`,
  `TestServerSharedCoverage_FailurePath` and
  `TestClientSharedCoverage_FailurePath` in `provider-sdk/runtime`.
- The host and the YDB provider list the feature.
