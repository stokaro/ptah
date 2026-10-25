# ADR 0036: A provider claims YAML entry keys and HCL blocks

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Amends: [ADR 0025](0025-a-provider-declares-the-source-syntax-it-owns.md)

Relates to: [ADR 0027](0027-a-provider-reads-the-sql-schema-documents-of-its-own-dialect.md)

Contract: 25.3, with ADRs 0033, 0034, 0035 and 0037.

## Context

ADR 0025 decided that a provider declares the source syntax it owns, and that the
host lexes it and sends the parsed values. It covered Go annotations and left HCL
open. It also decided that a user who switches an engine to its provider keeps
every annotation and YAML key that worked. HCL documents are desired schemas
like the others, and the built-in YDB source reads blocks in them that a
provider-selected run cannot read. YAML is claimed at two scopes, and the
built-in YDB source uses more.

What the code does today:

- **YAML.** `YAMLSourceKey.Scope` has `DOCUMENT` and `TABLE`, and
  `provider/yaml_source.go` refuses any other. `internal/providerhost/yaml.go`
  turns a document key into a `yamlext.Section` and a table key into a
  `yamlext.EntrySection`, and refuses a decoded facet attached to anything but a
  table ("YAML facet attachment to non-table objects").
- **The built-in YDB YAML source** (`internal/ydbsource/yaml.go`) uses more. It
  adds keys to an index entry through `yamlext.EntryAttributes`: partitioning,
  vector settings and full-text or local options, and it reads the entry's own
  `type` and `ops` (`Reads`) to know the index kind and metric. Its
  `column_families` table key lists member columns under `fields`, so what it
  declares belongs to columns of the table.
- **Go annotations already have the pieces YAML lacks.** `GoSourceDirective` has
  `COLUMN` and `INDEX` scopes, and `goFacetSubject` in
  `internal/providerhost/go_source.go` lets a table directive answer for a column
  of its own table, which is the column families case. A Go claim has no list of
  the common attributes it reads, so a claim on `ptah:schema:index` cannot
  receive the index's `type` and `ops`, which the built-in YDB source reads
  (`internal/ydbsource/attributes.go`).
- **HCL.** `core/hclext` is the contract between the HCL frontend and an owner.
  An owner registers the shape of each block it reads: its site (top level,
  table attributes, a block in a table, index attributes, routine attributes,
  materialized view attributes), type, label naming, attributes and their
  kinds, children, and a grant kind. The frontend evaluates every attribute and
  hands the owner data, never HCL syntax. The built-in YDB source registers a
  top-level `coordination_node` block and attributes on the frontend's `index`
  block (`internal/ydbsource/hcl_extension.go`), reads a `ptah:not-described
  coordination_node` header entry and a captured `ptah:feature-coverage` header.
- **The SDK decodes HCL and claims none.** `ValidateSourceDocuments` accepts HCL
  documents with `SourceBlock` content, and the fixture decodes an HCL retention
  block. `Engine` has no HCL claim, so `providerhost.Host.Provider()` sets no
  `HCL` extension, and a provider-selected run reads no provider block.

## Decision

### YAML: column and index entries

`YAMLSourceKey.Scope` gains `COLUMN = 3` and `INDEX = 4`, with the numbers Go
uses. A column key is a key of a column entry of a table (`columns` or
`fields`). An index key is a key of an index entry, of a table's `indexes` or of
the top-level `indexes` with its `table`. The host's YAML frontend keeps the keys
of a selected owner on a column entry as it keeps them on a table and an index
entry today.

A claim can name the common keys of its entry that the owner reads with it:

```proto
// YAMLSourceKey gains: repeated string inherited = 5;
// GoSourceDirective gains: repeated string inherited = 6;
```

The host copies those keys of the entry into `SourceFragment.inherited_attributes`.
They stay the host's keys: the host reads them as it always does, and the owner
reads them too, such as an index's `type` and `ops`. An inherited name is a
common key of the claim's entry, and never a key the claim itself owns.

The binding rules follow the Go ones:

- A `COLUMN` or `INDEX` fragment has a common table binding with an entry key
  and a `subject` of kind `column` or `index`: the table's schema, the table as
  the only path component, and the entry's name.
- A decoded facet of a `COLUMN` or `INDEX` fragment belongs to that subject. The
  host refuses one for another subject.
- A decoded facet of a `TABLE` fragment belongs to its table or to a column of
  that table, as a Go table directive's may. The host refuses a column of another
  table and a name the table does not declare.

### HCL: block claims

An engine declares the HCL it reads:

```proto
// Engine gains: HCLSourceDeclaration hcl_source = 17;
message HCLSourceDeclaration {
  repeated HCLSourceBlock blocks = 1;
  // Kinds a ptah:not-described header entry may name, which the provider
  // reads as limits on its own claim.
  repeated string header_kinds = 2;
}
message HCLSourceBlock {
  enum Site {
    SITE_UNSPECIFIED = 0;
    TOP_LEVEL = 1;
    TABLE_ATTRIBUTES = 2;
    IN_TABLE = 3;
    INDEX_ATTRIBUTES = 4;
    ROUTINE_ATTRIBUTES = 5;
    MATERIALIZED_VIEW_ATTRIBUTES = 6;
  }
  enum Naming {
    NAMING_UNSPECIFIED = 0;            // every site but TOP_LEVEL
    UNLABELED = 1;
    SCHEMA_AND_NAME = 2;
    SCHEMA_AND_QUALIFIED_NAME = 3;
    JOINED_NAME = 4;
  }
  Site site = 1;
  string type = 2;                     // "index" or "table" at their sites
  Naming naming = 3;
  repeated HCLSourceAttribute attributes = 4;
  repeated HCLSourceChild children = 5;
  bool declares_type = 6;
  // The kind a permission block's `for` names an object of this block as.
  string grant_kind = 7;
  string selector = 8;
  repeated string models = 9;          // DESIRED models with source format HCL
  repeated string inherited = 10;      // common attributes the owner reads
}
message HCLSourceAttribute {
  enum Kind {
    KIND_UNSPECIFIED = 0;
    STRING = 1;
    TEXT = 2;
    TEXT_LIST = 3;
    BOOL = 4;
    REFERENCE = 5;
    REFERENCES = 6;
    SQL = 7;
    TYPE_NAME = 8;
  }
  string name = 1;
  Kind kind = 2;
  string refers = 3;                   // the block type of a reference
  bool optional = 4;                   // only for REFERENCE
}
message HCLSourceChild {
  string type = 1;
  bool unlabeled = 2;
  repeated HCLSourceAttribute attributes = 3;
}
// SourceFragment gains: HCLSourceBlock hcl_block = 12;
// SourceDocument gains: repeated SchemaCoverageLimit limits = 8;
```

The fields have the meaning of `hclext.Block`, `hclext.Attribute` and
`hclext.Child`, so the host adapter builds an `hclext.Extension` from them
without reading engine knowledge. The host checks a block against its shape and
evaluates it before dispatch, as it does every block, and the frontend's refusals
and tolerance apply to a provider's block the same way.

A fragment carries the frozen claim in `hcl_block`, its selector, and content by
site:

| Site | Content | Binding and subject |
| --- | --- | --- |
| `TOP_LEVEL` | `block` | None; the labels name the object |
| `IN_TABLE` | `block` | The table |
| `TABLE_ATTRIBUTES` | `value`, a mapping | The table |
| `INDEX_ATTRIBUTES` | `value`, a mapping | The table, and the index as subject |
| `ROUTINE_ATTRIBUTES` | `value`, a mapping | The routine as subject, with its overload |
| `MATERIALIZED_VIEW_ATTRIBUTES` | `value`, a mapping | The view as subject |

An attribute mapping holds the owner's attributes only, and the inherited ones
travel in `inherited_attributes`: an index's `type` and its operator class. A
value keeps its evaluated case: text, a bool, a list of text, a `Reference`, or
the SQL of an `sql()` call. `SourceDocument.limits` carries the header entries of
the engine's `header_kinds`, in written order, and appears only on an HCL
document.

A captured `ptah:feature-coverage` header is the exact claim an export wrote. The
host decodes it with the provider's model definitions, which it already holds, and
uses it as the document's claim for the provider's kinds. The provider takes no
part in it.

### Ownership and refusals

- A block type at the top level, and an attribute name at a site, belong to one
  selected owner. The host refuses a provider claim that collides with the
  frontend's own blocks or with another owner, as it refuses a YAML key.
- The SDK refuses a claim without a type, selector or models; a naming at a site
  other than `TOP_LEVEL` or none at `TOP_LEVEL`; children or a grant kind outside
  `TOP_LEVEL` and `IN_TABLE`; a reference attribute without `refers`, `optional` on
  any other kind; an attribute or child named twice; two claims of one site and
  type; and an HCL model that no claim names.
- The SDK refuses a fragment whose claim is not one the engine declares, whose
  selector differs, whose content does not fit its site, whose attributes or
  children are not in the claim, or whose binding or subject is missing.
- A provider refuses a block it cannot read with a positioned refusal; it never
  skips one.

### Negotiation

Both additions are two-way. `source.entry-keys` enables the `COLUMN` and `INDEX`
YAML scopes and the `inherited` lists; `source.hcl` enables `Engine.hcl_source`,
`SourceFragment.hcl_block` and `SourceDocument.limits`. A provider lists each in
`Description.features` and in each engine that uses it, beside `source.decode`.
The SDK drops the declarations a host did not ask for, and a host that does not
list the feature reads no provider block or entry key.

### Versioning

Both are compatible additions, part of contract 25.3. A provider that claims HCL
does not raise its minimum contract: an older host reads its YAML and Go claims
and refuses an HCL block it does not know, as it does today.

## Alternatives considered

**Send the HCL text.** The provider would parse its blocks. Variables, locals,
functions and the confinement of `file()` are the frontend's, so the provider
would need a second evaluator, which ADR 0022 rejected. Rejected.

**A generic block shape without sites.** A claim would name a block type and the
host would send whatever block matches. Index and table attributes extend the
frontend's own blocks rather than adding blocks, and the frontend has to know
which attributes of its `index` block are the owner's before it refuses unknown
ones. Rejected; the sites are the frontend's existing places.

**Decode column families as a table facet only.** The provider would keep the
host's model of families as one table facet. It ties the provider to a host
model, which ADR 0025 rejected for facets. Rejected; a table fragment may
answer for its columns, as the Go adapter already lets it.

## Consequences

- `YAMLSourceKey`, `GoSourceDirective`, `SourceFragment`, `SourceDocument` and
  `Engine` grow fields, and the SDK grows the HCL claim messages and their
  validators.
- The host grows an HCL adapter in `internal/providerhost`, column entry keys in
  `core/yamlschema`, and the column and index attachment of decoded YAML facets.
- Every built-in YDB source input has a home in a provider claim: YAML, Go
  annotations, HCL and YQL. The parity corpus that decodes each through both paths
  can cover HCL too.

## Acceptance criteria

These are for the implementation. None holds today.

- The SDK carries the YAML scopes, the `inherited` lists and the HCL claim as
  contract 25.3, and both boundaries refuse each case under "Ownership and
  refusals". In-process tests negotiate both features in both directions, and each
  validator is mutation-checked.
- The fixture provider claims a YAML index key with an inherited `type`, a
  top-level HCL block and an HCL index attribute, and SDK process tests in Ptah's
  CI decode each with positions and a refusal.
- The host adapters read a provider's YAML column and index keys and HCL blocks
  through the fixture provider, and attach their facets.
- The YDB provider declares the keys and blocks the built-in YDB source reads, and
  its CI decodes the source parity corpus, HCL included, through both paths on a
  current Ptah, with the same desired schema and the same refusals.
