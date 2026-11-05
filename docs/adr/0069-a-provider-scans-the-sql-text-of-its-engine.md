# ADR 0069: A provider scans the SQL text of its engine

Status: Proposed

Contract: 25.26 (provisional: renumbered if an earlier claim merges first)

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0027](0027-a-provider-reads-the-sql-schema-documents-of-its-own-dialect.md),
[ADR 0055](0055-a-provider-splits-a-migration-body-into-the-units-its-engine-runs.md)

## Context

Many host readers take SQL text apart without parsing it. They find where a
string or a quoted name ends, where a statement ends and which words a body
uses. They all build their lexer through `internal/dialectlexer`, which reads
the lexical section of the dialect's descriptor. The readers include the
lineage of view bodies (`ptah schema lineage`), the indentation of the `sql`
template function of `ptah-compat`, placeholder rewriting, statement
splitting, the safety assessment of statements, the transaction requirement
scan, the Ptah directive scan of migration bodies, and name reading in
`internal/sqlident`.

For YDB the descriptor named a lexer of its own, `descriptor.LexerYQL`. Its
scanner is not a set of switches over the shared rules. Both quote styles
delimit strings. `@@...@@` is a multiline string in which `@@@@` stands for
`@@`. A type suffix belongs to the string it follows (`"x"u`). A `--!` comment
at the head of the text is a translation setting the server reads. `$name` is
never a dollar-quoted string. The descriptor was registered in every process,
so the readers used this scanner for YDB even when the YDB provider served the
command.

Deleting the built-in engine (stokaro/ptah#5298) deletes the descriptor and the
scanner. ADR 0027 keeps the YQL grammar out of the host, so the scanner cannot
stay there. After the deletion every reader above reads YQL by the shared
rules. Measured with `ptah schema lineage --dialect ydb --format json` over one
YQL file: a table `shop/items` and five views. The views read it by its path
and include `"fixed" AS tag` and `@@it's@@ AS note`. The YDB provider was
`stokaro/ptah-ydb-plugin` 07068a0:

| Host | Edges | Undecided |
| --- | --- | --- |
| master 51812210f, built-in engine | 9, sources `shop.items` and `shop.active` | none |
| master 51812210f, the provider through `--provider-config` | the same 9 | none |
| #5298, the provider | 4, sources `shop/items` and `shop/active` | `shop.everything` (the table has no declared columns), `shop.noted` (no top-level FROM) |

The `@@` string's apostrophe opens a string under the shared rules, and that
string swallows the FROM clause. A source written as one quoted path is read as
a table name nobody declares. The provider has the YQL scanner: the plugin's
`internal/yql` holds the rules the host deleted, extracted unchanged.

## Decision

### The provider scans the text

Contract 25.26 adds the RPC `ScanSQL`. The feature `sql.scan` enables it. The
provider and each engine that scans list it. A host that does not know the RPC
never calls it, so the host lists no feature.

```proto
rpc ScanSQL(ScanSQLRequest) returns (SQLScan);

message ScanSQLRequest {
  string engine = 1;
  string text = 2;
  // The text begins where the server reads settings ahead of a query.
  bool head = 3;
}
message SQLScan {
  repeated SQLToken tokens = 1;
}
message SQLToken {
  enum Kind {
    KIND_UNSPECIFIED = 0;
    UNKNOWN = 1;
    STRING = 2;
    COMMENT = 3;
    SEMICOLON = 4;
    WHITESPACE = 5;
    IDENTIFIER = 6;
    OPERATOR = 7;
  }
  Kind kind = 1;
  // The byte offset in text at which the token ends. It starts where the
  // token before it ends, and the first one starts at 0.
  uint32 end = 2;
  // STRING: the text the literal stands for. IDENTIFIER: the name with its
  // quoting undone. Absent where the scanner reads no such value.
  optional string value = 3;
}
```

The kinds are the host lexer's token types. `UNKNOWN` is text the server
reads that is no other kind, such as a translation setting. A reader that
removes comments must keep it. A name, a keyword and a number are
`IDENTIFIER`, as the shared lexer reads them. The tokens cover the text with
no gap and no overlap, so a token needs only its end. The scan reads the text
alone: no session, no server version and no database.

`head` is set for a whole text and cleared for a range inside one. A range
starts after the head, so a translation setting there is a comment.

### What the SDK enforces

Both transport boundaries refuse:

- a request whose engine does not list the feature;
- a reply whose tokens do not cover the text exactly;
- a token of no kind or of an unknown kind;
- an empty token;
- a token that ends inside a UTF-8 sequence;
- a value on a token that is neither a string nor an identifier.

`provider.Server.Validate` refuses a description that lists the feature
without a scanner, and a scanner the description does not advertise. An
engine that lists the feature needs the provider's feature.

### What the host does

While a provider is selected, the host registers a scanner for each engine
that lists `sql.scan`, under its ID and its aliases. `dialectlexer.Options`
answers it for a dialect the process registry does not describe. Every reader
above then reads the engine's text with the provider's tokens. No reader
changes for this. The scanner caches each answer by text and head, and it
answers `StringValue` and `IdentifierValue` from the values of the scans it
made.

The host also registers the quoting the engine's query dialect declares
(`QueryDialect.quoting`, contract 25.1) with `internal/sqlident`. A provider
engine's name is then quoted and read as its engine writes it. For an engine
whose quoting has `paths`, lineage reads a FROM source that is one quoted
path as the table at that path. The path's directory is the schema, which is
how Ptah names such a table.

A scan that fails reads the rest of the text as one `UNKNOWN` token. No reader
takes that token for a string, a name or a separator. The host does not fall
back to the shared rules, because they answer for another language.

### What the YDB provider sends

The provider lists the feature for `ydb`. It answers with the scanner of
`internal/yql` and the value decoders beside it, which are the rules the
built-in engine held.

## Alternatives considered

**Declare the lexical rules as data.** The engine description could carry
switches like those of `descriptor.Lexical`: which quotes delimit strings, a
multiline delimiter, an escape rule. The shared lexer would read them, with no
call per text. YQL's rules are not switches of the shared lexer: type
suffixes, the head of the text, and `@@@@` inside a multiline string would
each be a new switch, and the next engine would add more. The shared lexer
would become a union of engines' rules. ADR 0027 already rejects that shape
for the grammar. Rejected.

**Keep the YQL scanner in the host.** It is 700 lines, with no dependency outside the standard library. It
is the engine knowledge ADR 0027 moves into the provider, and keeping it would
leave the host one YQL change behind every provider release. Rejected.

**A lineage service only.** The provider could return the edges of its views.
That restores one reader of the eight, and each other reader would need a
service of its own. Tokens serve all of them. Rejected.

**Scan only when a reader asks for it.** Each reader would choose between the
provider's tokens and the shared rules. A reader added later would read the
engine's text by the wrong rules until someone noticed, which is the failure
this record measures. Rejected.

## Consequences

- Contract 25.26 adds one RPC, three messages and a one-way feature. A provider
  that lists the feature still runs on a host of an earlier minor. That host
  reads its text by the shared rules, as it does today.
- The SDK release comes first. The host and the providers adopt it in their
  own changes.
- Reading the text of a provider engine costs one call per distinct text. The
  cache holds repeated texts.
- An engine whose text the shared rules read correctly gains nothing from the
  feature and need not list it.

## Acceptance evidence

- SDK tests accept a covering scan and refuse each malformed reply and request
  above. The server refuses a feature without a scanner and a scanner without
  the feature.
- Through the SDK fixture provider, `ptah schema lineage` resolves a view over
  a quoted path whose body holds a string the shared rules misread. A provider
  that does not list the feature leaves the view undecided.
- Through the YDB provider with the host of stokaro/ptah#5298, the lineage
  measurement above returns the nine edges master returns.
