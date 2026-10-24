# ADR 0027: A provider reads the SQL schema documents of its own dialect

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0021](0021-provider-recognition-does-not-authorize-execution.md),
[ADR 0023](0023-engine-knowledge-moves-with-the-provider.md),
[ADR 0025](0025-a-provider-declares-the-source-syntax-it-owns.md),
[ADR 0026](0026-official-providers-are-pinned-in-the-release-installed-explicitly-and-bundled-by-the-distribution.md)

## Context

A desired schema can be written as a SQL file (`--schema-file schema.sql`, a
`.sql` file in a schema directory, or a SQL source in a project file). For YDB
that file is YQL, which the shared SQL grammar cannot read: variables, `UPSERT`,
`WITH (...)` table settings, `CREATE TOPIC`, `CREATE SECRET` and `GRANT` on
scheme objects are all outside it. Today the host reads such a file in four
steps, all inside the host module:

- The descriptor of the YDB target names the YQL lexer
  (`descriptor.LexerYQL`). `internal/syntaxext/yql` serves it with
  `internal/yqlparse` (22 non-test files, 2.2k lines), which turns text into
  `core/ast` nodes.
- `internal/ydbsql` (946 lines) registers a `sqlext.Extension` with a `Grammar`.
  The seam is in `core/sqlext`: a `Grammar` reads "the whole of every document of
  its targets, which are then not written in the shared grammar", and one target
  takes one grammar. `ReadSQL` receives every file of a load in order and returns
  a `sqlext.GrammarResult`: a `schemamodel.Database` with each file's changes
  applied, plus per-file declarations for the directory check.
- Conversion of tables, columns, indexes and views goes through
  `sqlschema.ConvertScript` in `internal/sqlschema`. `ydbsql` only converts the
  statements that shared conversion does not model. The package comment says what
  that means for an extracted provider: "an extracted YDB provider needs a public
  conversion library, or its own conversion of YQL tables".
- `ydbsource` supplies the header kinds a `ptah:not-described` header may name
  (`Header`) and the coverage claim a document makes (`Coverage`). The host passes
  the header entries in `sqlext.CoverageRequest.Limits`.

The SDK has no operation for this. `DecodeSource` accepts `SourceDocument` with
format `SQL`, but its input is a `SourceFragment` that the host's shared grammar
already cut out of a document written in the shared grammar. That fits a dialect
that adds a few statements to the shared grammar. It cannot read a document that
is not in the shared grammar at all. `AnalyzeSQL` and `SplitLintStatements` find
statement boundaries and safety; they return no schema. The plugin cannot reach
`sqlschema.ConvertScript` either, because it sits behind `internal/`.

So a provider-selected run has no way to take a YQL schema file, and the YQL
parser stays in the host. ADR 0025 declares the Go syntax a provider owns, and
says nothing about SQL documents. This decision closes that gap in a way that
suits any dialect with its own grammar (a CQL-like or a ClickHouse-like dialect
would need the same).

## Decision

### Two ways a provider takes part in SQL documents

An engine declares one of two modes in its description. A target has exactly one
mode, which keeps the existing rule that one target takes one grammar.

- `FRAGMENTS` is the existing mode. The host reads the document with the shared
  grammar and sends the statements it does not model to `DecodeSource` as `SQL`
  fragments. The host owns lexing, as ADR 0025 describes for Go.
- `DOCUMENT` is new. The provider reads every SQL document of the engine in full.
  The host does not lex or parse the text, because there is no shared grammar to
  lex it with. ADR 0025 says "the host does the lexing" for comments, and that
  statement holds where a shared grammar exists. This ADR adds the exception: a
  dialect with its own grammar owns the whole document.

### A new offline operation

A provider that declares the feature `schema.read-sql` and the mode `DOCUMENT`
implements one new RPC.

```proto
rpc ReadSQLSchema(ReadSQLSchemaRequest) returns (SQLSchema);

message ReadSQLSchemaRequest {
  string engine = 1;
  repeated string target_features = 2;
  ComparisonIdentifiers identifiers = 3;
  // Database root that absolute object paths in the documents resolve against.
  string database_path = 4;
  // Documents in the order the load runs them.
  repeated SQLDocument documents = 5;
  // Entries of the documents' ptah:not-described headers whose kind the engine
  // declared in sql_schema.header_kinds, in written order.
  repeated SchemaCoverageLimit limits = 6;
}
message SQLDocument {
  string id = 1;
  string path = 2;  // for diagnostics only; the provider never opens it
  string text = 3;
}
message SQLSchema {
  bool complete = 1;
  repeated DeclaredTable tables = 2;
  repeated Envelope native_objects = 3;  // DESIRED values of provider-owned models
  repeated SQLDocumentDeclarations declarations = 4;
  ModelCoverage coverage = 5;
  repeated SQLDiagnostic diagnostics = 6;
}
```

`Engine` gains `sql_schema`, a message holding the mode and `header_kinds`.
`DeclaredTable` is the message `ValidateSchema` and `PlanFeatures` already carry,
so the provider's answer enters the same comparison path as a Go or YAML schema.
Each declared object carries a `SourceTrace`, the existing type, so later
diagnostics point at a file and line of the original document.

`SQLDocumentDeclarations` mirrors `sqlext.FileDeclarations`: for each document the
objects it leaves declared, the objects it declares under a clause that admits an
existing object, and its `CREATE TABLE` and `DROP TABLE` statements in order. The
host uses these only for the check that a later file of a schema directory does
not declare again what an earlier one did. The provider owns the meaning of the
statements, and the host owns the directory rule.

The reply carries the result of applying all documents in order, as the host's
`GrammarResult.Database` does. A later document can alter what an earlier one
declared (`ALTER TABLE ... ADD CHANGEFEED` is the YDB case), so the provider has
to see every document in one call.

The common model has more than tables. Views are common in the host's model and
`DeclaredTable` does not hold them. The implementation of this ADR starts by
listing every field of `schemamodel.Database` that YQL conversion fills today and
giving each one a home in `SQLSchema`: an existing declared message, a new
declared message in the SDK (views, sequences, comments on objects), or an
owner-native envelope. A field with no home is refused by name rather than
dropped.

### Host responsibilities

`internal/providerhost` builds one `sqlext.Extension` per `DOCUMENT` engine
and puts it in `engine.Provider.SQL`:

- `Owner` is the provider ID and `Targets` the engine.
- `Grammar.ReadSQL` calls `ReadSQLSchema` and maps the reply into
  `sqlext.GrammarResult`. Mapping uses the codecs the provider already declares.
- `Header` is the engine's `header_kinds`. `Coverage` returns the `coverage` of the
  reply, so the claim a document makes about unmanaged objects comes from the same
  call that read it.
- The host reads the files. It applies the project-directory confinement that
  `file()` and `--schema-file` already apply, and sends text. The provider is given
  no path to open and no database connection.

The host checks the reply before use: one entry per requested document, trace
spans inside the document text, owners and kinds the engine declared, and
`complete=true`. A reply that fails a check is an error, never a partial schema.

Selection follows ADR 0026. A `.sql` desired schema for a `ydb://` target starts
the provider that rules 1 to 3 select. If none is found the command fails with
`provider_missing`. It does not parse the file with another grammar.

### Trust and authorization

The operation is offline. It takes no session, no address and no credential, so it
widens nothing ADR 0021 already authorizes: the process was started because the
operator installed a distribution, ran `providers install`, or wrote a manifest
entry. The documents are text the operator pointed at. The provider must not
write the document text into a diagnostic beyond the span and a bounded token,
since a schema file can carry a secret value in a statement.

### Atomicity and errors

There is nothing to commit. The call is a pure function of its request. A
canceled call returns no result, and the host does not retry it.

A failure is a gRPC error of the SDK's existing refusal kinds when the batch is
unusable, or a successful reply with `complete=false` and diagnostics. Each
`SQLDiagnostic` has a code (`SYNTAX`, `UNSUPPORTED`, `INVALID`, `INCOMPLETE`), a
document ID, a start and end position (byte offset, line, column), and a
message. The host prints the first error with its position in the same layout the
shared parser uses, and `--format json` carries all of them. `UNSUPPORTED` is a
construct the provider recognizes and cannot represent. It is refused by name,
never skipped.

### Versioning

The change bumps `provider.Revision`. The SDK maintainer tags the next
`provider-sdk/v0.N.0` and bumps the pin in `go.mod` of the host, the fixture and
the plugin in the pull requests that use it, as `provider-sdk/README.md` says.
The SDK is experimental, so the change breaks no promise.

The independent fixture module under `provider-sdk/testdata/fixture` declares
`DOCUMENT` mode for its narrow table grammar, so the operation
has a conformance subject that is not YDB.

### What moves

`internal/yqlparse`, `internal/ydbsql`, `internal/syntaxext/yql`, the YQL part of
`internal/sqlschema` conversion that only YDB uses, and `descriptor.LexerYQL` move
into the plugin or are deleted. Any other host caller of `yqlparse` is found by
`git ls-files` and `go list -deps` and moves with it. The YQL lint lexing already
belongs to the provider under ADR 0023.

## Amendments

ADR 0025 is consistent with this decision. One sentence needs an amendment when
this ADR is accepted: "the host does the lexing" applies to dialects that share
the host's grammar. The amendment is the `DOCUMENT` mode above. This ADR does not
edit ADR 0025.

ADR 0025 covers Go annotations. HCL schema documents have the same gap (the
description lists no HCL claim and `ydbsource` has `hcl.go`). This ADR does not
decide HCL. It is an open question below.

## Alternatives considered

**A public conversion library.** Export `sqlschema.ConvertScript` and the
`schemamodel` types so the plugin builds a `schemamodel.Database` itself. The
`ydbsql` comment suggests it. It makes the host's internal model a public,
versioned contract, which ADR 0025 rejected for facets, and it ties every
provider to the host's Go types. A non-Go provider could not use it. Rejected.

**Send AST nodes.** The provider parses and returns `core/ast` nodes, and the host
converts them. That moves the lexer and leaves YQL conversion in the host, with
the YDB statement shapes it needs. The host would still know the engine's
statements. Rejected.

**Extend the `FRAGMENTS` mode with a YQL-aware shared lexer.** Teach the shared
grammar enough YQL to cut fragments. It puts a second dialect into a grammar that
is shared by design, and the cuts would still need YQL knowledge. Rejected.

**Convert YQL to the host's neutral SQL in the plugin and send it back.** The host
parses the result. The round trip loses native statements and spans. Rejected.

## Consequences

- The SDK grows one RPC, one engine declaration and a few declared messages.
- The plugin takes over about 3k lines of parser and conversion, and their tests.
  The YQL corpora move with them.
- The host keeps the generic parts: file reading and confinement, the directory
  check, header parsing, and the mapping from the wire to `GrammarResult`.
- A user who runs `schema plan --schema-file` against YDB starts the provider
  process even for an offline plan. That is the cost of owning the grammar, and
  the `provider_missing` diagnostic names the install command.
- The differential corpus is large. It is the main cost of proving parity.

## Acceptance criteria

These are for the future implementation. None holds today.

- `ReadSQLSchema` is in `provider.proto`, the SDK interface and both transport
  boundaries, with validators that refuse a reply with a wrong document count,
  a span outside its text, an unknown owner or kind, and `complete=false` without
  an error diagnostic.
- SDK conformance tests in `provider-sdk/integration` run the operation against
  the fixture provider: load order, a later document that alters an earlier one,
  the directory-check declarations, diagnostics with positions, cancellation, and
  a refused batch exposing no prefix.
- A differential corpus feeds every YQL input from `internal/yqlparse` and
  `internal/ydbsql` tests, the examples under `examples/`, and the fixtures under
  `integration/internal/fixtures/entities/` that include YQL (enumerated with
  `git ls-files`, with a floor on the count) through the bundled implementation
  and through the plugin. The recorded `schemamodel.Database` goldens are equal
  after host mapping. A refused input is refused by both with the same construct
  named.
- A mutation test: disabling the plugin's handling of a statement makes the
  corpus fail on the rows that use it.
- A host end-to-end test in `integration/` runs `ptah schema plan --schema-file`
  and `ptah schema apply` with a `.sql` file against YDB through the bundled
  provider and reads the table back.
- A guard reports any non-test importer of `internal/yqlparse` outside the plugin
  after the flip, using `go list`, and fails if the package list it checks falls
  below a floor.
- A field audit test lists every `schemamodel.Database` field that YQL conversion
  fills, and fails when a field has neither a wire home in `SQLSchema` nor a named
  refusal.
- `docs/site/src/content/docs/extend/executable-providers.md` documents
  `schema.read-sql`, the two modes and the diagnostics.

## Decided questions

- The field audit of `schemamodel.Database` against `SQLSchema` is part of the
  implementation, and the acceptance criteria below name it. Views and any other
  common object without a wire home get a declared SDK message or a named refusal.
- HCL desired-schema documents for a provider engine follow in a later record that
  reuses the claim pattern of ADR 0025. The field audit also lists every YDB HCL
  input. If one has no YQL or Go equivalent, HCL support becomes a flip
  precondition.
