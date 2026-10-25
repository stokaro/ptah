# ADR 0037: A provider encodes the source it decodes

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0025](0025-a-provider-declares-the-source-syntax-it-owns.md),
[ADR 0036](0036-a-provider-claims-yaml-entry-keys-and-hcl-blocks.md)

Contract: 25.3, with ADRs 0033 to 0036.

## Context

Ptah writes desired-schema documents as well as reading them. A value a provider
owns has to survive that writing, or a document Ptah exports from a YDB database
cannot be read back into the same schema.

What the code does today:

- **The writers ask each owner for its part.** `hclext.Encoder`
  (`core/hclext/encode.go`) returns an owner's blocks and the attributes it adds
  to the writer's own table, column, index, key, routine and materialized view
  blocks, as data with losses; `internal/atlashclrender` quotes, references and
  places them. `annotation.Encoder` (`core/annotation/encode.go`) returns
  directives, attributes on the frontend's own directives, not-described
  declarations and Go field types; a value it cannot write refuses the export.
- **The commands that write.** `ptah schema inspect --format hcl`
  (`internal/cli/schema/inspect.go`, through `internal/atlasreport/schema_inspect.go`),
  `ptah schema export --format hcl` (`internal/cli/schema/schema.go`), the
  compatibility `schema inspect` (`internal/cli/atlas/schema_inspect.go`), and
  `ptah introspect` (`internal/cli/introspect`, through
  `internal/convert/goschematogo`).
- **The built-in YDB source writes both formats.** `internal/ydbsource/hcl_encode.go`
  writes coordination nodes and index settings back as the blocks and attributes
  it reads, and reports the table settings HCL has no spelling for as losses at
  their table. `internal/ydbsource/annotations_encode.go` writes the YDB
  directives and gives an export's fields the Go types of their YDB types.
- **A provider has no encoder.** The SDK has `DecodeSource` and nothing in the
  other direction. `providerhost.Host.Provider()` builds an annotation extension
  with no `Encoder`, and no HCL extension. So `goschematogo` refuses a provider
  facet as one "a Go annotation cannot represent", and the HCL writer reports
  every provider object as a loss.

ADR 0036 gives the provider claims in every format the host writes. This record
is their mirror.

## Decision

### One offline call

A provider that lists `source.encode`, in `Description.features` and in each
supporting engine's features, implements:

```proto
rpc EncodeSource(EncodeSourceRequest) returns (EncodedSource);

message EncodeSourceRequest {
  string engine = 1;
  repeated string target_features = 2;
  SourceDocument.Format format = 3;     // YAML, HCL or GO_ANNOTATIONS
  // The document's tables, with the frontend naming they are written with.
  repeated SourceBinding bindings = 4;
  repeated EncodeSourceValue values = 5;
  // What the document knows of the provider's models (DESIRED).
  ModelCoverage coverage = 6;
  // GO_ANNOTATIONS only: the columns of the bound tables, for their Go types.
  repeated EncodeSourceColumn columns = 7;
}
message EncodeSourceValue {
  SourceModelValue.Placement placement = 1;
  NormalizedValue value = 2;            // DESIRED; its owner is the subject
  string binding_id = 3;                // the table of a facet
}
message EncodeSourceColumn {
  string binding_id = 1;
  string name = 2;
  string type = 3;                      // the engine type
  bool nullable = 4;
  bool primary_key = 5;
}
message EncodedSource {
  bool complete = 1;
  repeated EncodedFragment fragments = 2;
  repeated EncodeLoss losses = 3;
  // The ptah:not-described entries the document has to carry.
  repeated SchemaCoverageLimit limits = 4;
  repeated EncodedFieldType field_types = 5;   // GO_ANNOTATIONS only
}
message EncodedFragment {
  // The claim, selector, binding, subject and content, as DecodeSource
  // receives them, with no positions.
  SourceFragment fragment = 1;
  repeated string value_ids = 2;        // the input values it writes
  enum Placement {                      // HCL TOP_LEVEL and IN_TABLE only
    PLACEMENT_UNSPECIFIED = 0;
    BEFORE_TYPES = 1;
    USER_TYPES = 2;
    AFTER_TABLES = 3;
    AFTER_TRIGGERS = 4;
    WITHIN_TABLE = 5;
  }
  Placement placement = 3;
}
message EncodeLoss { string value_id = 1; string message = 2; }
message EncodedFieldType {
  string binding_id = 1;
  string column = 2;
  string go_type = 3;                   // such as "uint64" or "time.Time"
  string go_import = 4;
}
```

The reply is the input `DecodeSource` would read: each fragment carries the
negotiated claim of its format (`yaml_key`, `go_directive` or `hcl_block`), the
selector, the binding and subject the claim's scope needs, and the evaluated
content, a mapping or a block. Decoding the fragments gives the values back with
the same canonical projections. The host writes the syntax: quoting, references,
labels, comment markers and placement within a placement's group. The provider
never writes text of a format.

`SQL` is not a format of this call. A document written as SQL is a rendering of
statements, which ADR 0034 covers with the `TEXT` form of `Render`. YAML is one,
although the host writes no YAML document today, so that a YAML writer needs no
contract change.

### Every value is written or reported

Each input value is accounted for once: written by exactly one fragment, or
reported as one loss with a one-line message that names it and never its
content. The host reports an HCL loss where it writes the value's block, as it
reports the losses of its own owners. A Go export takes no loss, so the host
refuses the export at the first one, naming the value. A value of a model whose
source formats do not include the format is always a loss.

`limits` are the `ptah:not-described` entries that keep the document's silence
from reading as absence, such as a kind the read did not inspect. An HCL limit
names one of the engine's `hcl_source.header_kinds`.

### What the host sends

- Values are DESIRED. The host converts an inspected value with `ConvertValues`
  first, as it converts its own owners' values before writing.
- A facet names its table binding, and its owner reference names the table, or a
  column or an index of it with the table as the only path component.
- `columns` lists every column of every bound table on a Go export, so the
  provider can give a field the Go type of its engine type, as the built-in YDB
  encoder does.

### How the host adapts

`internal/providerhost` gains an `hclext.Encoder` and an `annotation.Encoder`
over `EncodeSource`. A `TOP_LEVEL` or `IN_TABLE` fragment becomes an
`EncodedBlock` with the fragment's placement; an attribute fragment becomes
`SlotAttributes` on the writer's block for its subject. A Go fragment of a
provider directive becomes an `EncodedDirective`, and one on a Ptah directive
becomes attributes of that slot. Field types become `FieldType` entries, and
limits become header entries or `EncodedLimit` declarations.

### Validation

Both transport boundaries refuse:

- a format other than YAML, HCL or GO_ANNOTATIONS, a value that is not DESIRED or
  whose model the engine does not declare, a facet without a binding of the
  request, and `columns` outside a Go export;
- a fragment whose claim is not one the negotiated engine declares for the
  format, whose selector differs, whose binding, subject or content does not fit
  its scope or site, that carries a position, that writes no value, or that
  writes a value of a model its claim does not name;
- a value written twice, written and lost, or neither;
- a placement on a fragment that is not an HCL `TOP_LEVEL` or `IN_TABLE` one, or
  none on such a fragment;
- an empty or multi-line loss, a limit of a kind the engine does not declare, and
  a field type for a column the request does not hold or for one column twice;
- `complete=false`. A canceled call returns nothing.

### Versioning

`source.encode` is a new service, part of contract 25.3. It adds no field to an
existing message, so it is not two-way: a host calls it only when the provider
lists it. Its replies use the claims ADR 0036 negotiates, and a reply that uses a
claim the host did not ask for is refused like any other. A provider does not
raise its minimum contract for it; an older host writes a provider's values as it
does today, as losses.

## Alternatives considered

**Write the provider's values as captured payloads.** The writer would embed the
encoded envelope in a comment the reader recognizes. A document would hold data
nobody can review or edit, and only the same provider release could read it.
Rejected.

**Let the provider write the text of its blocks.** The host would insert the
provider's HCL or Go text. Every provider would quote and reference differently,
and the writer could not place, order or check a block it cannot read. ADR 0025
put syntax mechanics in the host for the same reason. Rejected.

**One call per format.** `EncodeHCL` and `EncodeAnnotations` would mirror the host
interfaces. The formats already share one fragment shape on the decode side, and
two calls would double the validators. Rejected.

## Consequences

- The SDK grows one RPC and its messages. The validators reuse the decode side's
  claim and binding checks.
- `schema inspect --format hcl`, `schema export --format hcl`, the compatibility
  `schema inspect` and `ptah introspect` write a provider's values for every engine
  whose provider serves the call.
- A round trip, decode after encode, becomes a test every provider can run on its
  own corpus.
- The YDB provider takes over `internal/ydbsource/hcl_encode.go` and
  `annotations_encode.go`, which are deleted from the host at the flip.

## Acceptance criteria

These are for the implementation. None holds today.

- The SDK carries `EncodeSource` as contract 25.3, and both boundaries refuse each
  case under "Validation". In-process tests cover an accounted batch, a loss, a
  refused batch and cancellation, and each validator is mutation-checked.
- The fixture provider encodes its retention value in YAML, HCL and Go, and SDK
  process tests in Ptah's CI decode what it wrote and compare the canonical
  projections.
- The host writes a provider's values through the fixture provider in
  `schema inspect --format hcl` and `ptah introspect`, and reads the output back
  to the same schema.
- The YDB provider's CI exports a database holding every YDB object and setting
  through HCL and Go, reads each export back and plans nothing, on each YDB line it
  supports.
