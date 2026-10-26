# ADR 0045: A provider states common column comparison and readback semantics

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0033](0033-common-objects-reach-a-provider-through-the-common-wire.md)

Contract: 25.6.

## Context

A target can implement `core/schemacomparison.Columns` in process. The shared
comparator asks it about type equivalence, default equivalence, effective
nullability and sequence settings. Catalog conversion asks the same owner to
read a reported default as a literal. A subprocess target has no corresponding
service and falls back to common rules and descriptor facts.

For YDB, these omissions affect one connected operation. A primary key renders
as `NOT NULL` even when its source states nullable. Type aliases and serial
types need owner comparison rules. A default such as `5t` must read back as a
literal value that the renderer can write again. Source-to-source comparison
also needs the effective columns a `CREATE TABLE` produces. A nullable-only
preparation field would leave the type and default fixed points unresolved.

The measured embedded primary-key cases cover row and column tables with
nullable omitted, true and false. All render `NOT NULL`. The source field is
retained; comparison uses effective nullability. Table context matters because
storage and table-level keys can determine a column's effective definition.

## Decision

### One negotiated column-semantics feature

The two-way feature `schema.columns` declares an engine's complete support for:

- Offline batch comparison of requested common-column aspects.
- Offline batch interpretation of observed column defaults.
- Complete common-column predictions in its table-creation projection.

The feature requires table-creation projection. It applies to captured columns
of both common and provider-owned tables; it does not require or grant
`schema.common-table-identity.v1` planning authority. The provider and supporting
engine both list it. Negotiation removes the
feature for a peer that does not support it. A reply with the new projection
fields is refused when the feature was not negotiated.
A provider that needs these semantics requires contract 25.6. There is no
engine-name branch or embedded provider fallback in the host.

The services perform no server access. Requests carry the selected engine,
negotiated target features, identifier semantics and capability facts. A
missing fact needed to answer is a refusal, not an assumed default. Context
cancellation prevents dispatch and cancels in-flight calls.

### Compare exact columns inside captured parent tables

`CompareColumns` receives ordered table contexts. Each context contains its
structured desired and current `ComparisonIdentity` values, captured
`DeclaredTable` and captured `ObservedTable`, plus ordered column pairs. A pair identifies one declared
column and one observed column by their exact names in those parents. It also
carries structured desired and current column identities whose parent and
normalized components match their respective table contexts and columns.

The host chooses the pairs. The provider does not match columns, infer renames,
or interpret an omitted column as absent. A current column can have a different
name when the host has already paired a rename. Duplicate pairs, nonexistent
names, changed identities and parent-boundary mismatches are refused.

The host calls the service after table and key preparation. Authored source
fields remain available as captured declarations; receipts are effective
comparison facts and never replace the source model.

Each pair lists the exact aspects the host delegates:

- `COLUMN_TYPE`: only where the host would invoke `Columns.TypeChange`. Domain
  comparison remains with its existing owner.
- `COLUMN_DEFAULT`: the target's equivalence of the declared and reported defaults.
- `COLUMN_NULLABILITY`: the effective nullable value of the declared column.
- `COLUMN_SEQUENCE`: only when the target supports serial sequence options.

The reply repeats both desired and current identities exactly. It is complete
and preserves table and pair order. Every requested
aspect has exactly one receipt. Unrequested, repeated, missing and unknown
aspects are protocol errors. There is no success with a partial receipt.

Type and default receipts carry a required `SAME` or `CHANGED` state. A changed
receipt provides the current and desired diagnostic spellings. Empty default
spellings are valid. The host forms its existing change report and keeps the
original desired declaration as the operation operand.

The nullability receipt carries a required optional boolean: explicit false
means not nullable, and an absent field is an error. The host uses this fact
instead of descriptor primary-key inference and `AlwaysNotNull` inference for
that delegated column. Other comparison checks retain their existing owners.

The sequence receipt is present even when its delta list is empty. Deltas have
closed attributes `IDENTITY_START` and `IDENTITY_INCREMENT`, each at most once,
with current and desired spellings. The provider cannot add arbitrary change
keys or operations through this result.

`SERIAL RESTART` is not a comparison setting of this service. Restart commands
and rollback positions remain with the existing column-operation owner and
the captured `CurrentSequenceRestart` operand. A sequence receipt cannot
invent a restart or replace that execution state.

The protocol uses these request and receipt shapes:

```proto
message CompareColumnsRequest {
  string engine;
  ComparisonIdentifiers identifiers;
  map<string, bool> capabilities;
  repeated string target_features;
  repeated ColumnComparisonTable tables;
}
message ColumnComparisonTable {
  ComparisonIdentity subject;
  ComparisonIdentity current_subject;
  DeclaredTable desired;
  ObservedTable current;
  repeated ColumnComparisonPair pairs;
}
message ColumnComparisonPair {
  ComparisonIdentity subject;
  ComparisonIdentity current_subject;
  string desired_column;
  string current_column;
  repeated ColumnAspect aspects;
}
message ComparedColumns {
  bool complete;
  repeated ComparedColumnTable tables;
}
message ComparedColumnTable {
  ComparisonIdentity subject;
  ComparisonIdentity current_subject;
  repeated ComparedColumn columns;
}
message ComparedColumn {
  ComparisonIdentity subject;
  ComparisonIdentity current_subject;
  repeated ColumnAspectReceipt aspects;
}
message ColumnAspectReceipt {
  ColumnAspect aspect;
  oneof receipt {
    ColumnDifference difference;
    ColumnNullability nullability;
    ColumnSequenceDifferences sequence;
  }
}
message ColumnDifference {
  Relation relation; // SAME or CHANGED; unspecified is invalid.
  string current;
  string desired;
}
message ColumnNullability { optional bool nullable; }
message ColumnSequenceDifferences {
  repeated ColumnSequenceDifference changes;
}
```

### Read observed defaults without changing unrelated column facts

`ReadColumnDefaults` receives ordered entries, each with a structured observed
column identity and its full `ObservedColumn`. It receives all observed columns
whose defaults the caller converts, including columns absent from the desired
schema that become rollback operands. It does not require a desired column.

The complete ordered reply repeats each identity and provides one typed result:
`ABSENT`, `LITERAL` or `EXPRESSION`. A literal can be empty and sets
`Field.DefaultSet`. An expression retains SQL in `Field.DefaultExpr`. Absence
is valid only when the input has no default expression. Unknown kinds and
missing entries refuse the conversion. A missing receipt never invokes a
common fallback.

An `ABSENT` receipt must have an empty value. An `EXPRESSION` receipt requires
a nonempty reported input and a nonempty value. An explicit empty `LITERAL`
is valid and remains distinct from absence.

The host batches this call before `dbschematogo` conversion and before the
comparator constructs removed-column rollback operands through `catalogfield`.
The receipt overrides only default conversion. Column identity, type,
nullability, comments and other facts stay with their existing conversion.

```proto
message ReadColumnDefaultsRequest {
  string engine;
  ComparisonIdentifiers identifiers;
  map<string, bool> capabilities;
  repeated string target_features;
  repeated ObservedColumnDefaultInput columns;
}
message ObservedColumnDefaultInput {
  ComparisonIdentity subject;
  ObservedColumn column;
}
message ReadColumnDefaultsResult {
  bool complete;
  repeated ColumnDefaultReceipt columns;
}
message ColumnDefaultReceipt {
  ComparisonIdentity subject;
  DefaultKind kind; // ABSENT, LITERAL or EXPRESSION.
  string value;
}
```

### Predict the common columns a table creation produces

Under `schema.columns`, `ProjectedTableCreation` includes an ordered
`repeated ObservedColumn columns` receipt. It covers every column of the
captured declaration exactly once, in declaration order, with unchanged names.
There are no additional columns. Missing, duplicate and reordered columns are
refused. Each predicted column is validated with the existing observed-column
shape rules.

The prediction includes the effective type, nullability, typed default and
identity/sequence metadata. The host replaces its provisional common columns
with these predictions after common creation conversion. It retains the
existing facet ownership and coverage checks. A prediction remains a predicted
creation effect; it is not an inspection and establishes no observed absence.

This uses the same `ObservedColumn` vocabulary the provider already uses to
report a column. The provider owns the semantics of its rendered creation;
SDK validation proves accounting and shape, while fixed-point tests prove the
prediction agrees with rendering and inspection.

### Keep other column semantics with their owners

Generated-expression comparison remains the separate
`Rules.GeneratedExpressions` capability. This feature does not claim to compare
it. YDB refuses generated-column declarations through its schema validation
until it implements them. Foreign keys, uniqueness, comments and grants remain
with the common comparator or their existing feature owners.

## Alternatives considered

**Add only nullable adjustments to table preparation.** This solves one visible
symptom while type/default comparison and literal readback still disagree with
the created schema. Rejected.

**Call an RPC from each contextless `Columns` method.** Those hooks cannot carry
cancellation or return a refusal. They also encourage mutable caches whose keys
omit parent context. Rejected; host adapters use contextful batch boundaries.

**Replace the authored field with a normalized field.** This changes export and
operation operands and loses the user's declaration. Rejected; effective
receipts are separate from the captured source.

**Return arbitrary column change maps.** A provider could modify comparison
areas it was not asked to own. Rejected; aspects and sequence keys are closed.

## Acceptance

SDK tests cover negotiation, exact table/column binding, rename pairs, complete
ordered accounting, each requested-aspect combination, unknown receipt kinds,
missing false nullability, empty literals, absent defaults and cancellation.
Mutating only an identity or only a referenced column name must fail validation.

Host tests cover domain and sequence routing, default readback for observed-only
columns, rollback operands, source-to-source projection, and preserved authored
fields. A refused batch returns no partial schema or comparison.

YDB tests compare row and column primary keys with nullable omitted, true and
false; serial and alias types; typed numeric/string/temporal defaults; empty
string literals; and sequence start/increment changes. A rendered/applied/read
schema and its predicted creation must each compare without churn, while
intentional type/default/nullability/sequence changes remain visible.

The change follows separate ADR/SDK review, SDK release, host pin/adapters and
provider adoption. It does not authorize a provider release or the embedded YDB
switch before full parity evidence.
