# ADR 0115: A provider engine states the lexical rules of its text

Status: Proposed

Contract: 25.34

Deciders: Ptah maintainers

Issue: [stokaro/ptah#5754](https://github.com/stokaro/ptah/issues/5754)

Relates to: [ADR 0069](0069-a-provider-scans-the-sql-text-of-its-engine.md)

## Context

The host reads an engine's SQL text with its shared lexer wherever it takes
the text apart without a provider call: a SQL schema document in FRAGMENTS
mode, the directives of a migration body, lint, lineage and placeholder
rewriting. `internal/dialectlexer.Options` configures that lexer from the
lexical section of the engine's dialect descriptor. A provider engine has no
descriptor, so the lexer reads its text by the SQL standard's rules alone.

For a PostgreSQL-family engine that misreads text the server accepts. Measured
with host 76572d2b2 and stokaro/ptah-spanner-plugin 7666e83, a Spanner schema
file whose column default is `E'it\'s'`:

```console
$ ptah schema lineage --dialect spanner --schema-file plain.sql --provider-config providers.json --provider-engine spanner
error: error parsing schema file: read SQL schema file plain.sql:1:61: unsupported column attribute: S
```

The lexer reads `E`, then `'it\'`, then a string that opens at `s'`. Nested
block comments, continued strings and `U&'...'` fail the same way.

ADR 0069 gave a provider a way to hand the host its own scanner, `sql.scan`.
It does not fit here. The host takes an engine with a scanner of its own to
write a language of its own, as YQL is, and `internal/parser` refuses every
SQL schema document of such an engine. Measured with the same file and a
Spanner provider build that lists `sql.scan`:

```console
error: error parsing schema file: read SQL schema file plain.sql: unsupported feature: the shared SQL grammar does not read spanner documents; the owner of the target reads them
```

A PostgreSQL-family engine writes the shared grammar. What it lacks is the
switches a built-in descriptor sets.

## Decision

Contract 25.34 adds `QueryDialect.lexical`, a `QueryLexical` message, under
the two-way feature `query.lexical`. Its fields are the descriptor's lexical
section with the descriptor's JSON names, limited to the rules the shared
lexer reads:

- `backslash_escapes`, `escape_strings`, `continued_strings`,
  `unicode_strings` and `nested_comments`;
- `dash_comment_needs_space`, `executable_comments`, `bracket_identifiers`
  and `hash_names`.

The provider and each engine that states the rules list the feature, and so
does a host that reads them.

The SDK refuses:

- an engine that lists the feature and states no rules, or states rules
  without listing it;
- an engine that lists it without the provider's feature;
- an engine that lists both `query.lexical` and `sql.scan`, since a scanner
  reads the text in place of the shared lexer, as a descriptor that names a
  lexer of its own states no rule;
- an executable comment style that is not a word.

The host owns that style's vocabulary and refuses a value it does not know.

A host that does not list the feature receives the query dialect without the
rules, and the feature is removed from the engine. A host that does not list
`query.dialect` receives neither, since the rules travel inside the dialect.

While the provider is selected, the host registers the rules for the engine's
ID and aliases beside its quoting. `dialectlexer.Options` answers them for an
engine the process registry does not describe, so every reader of the
engine's text through the shared lexer reads the engine's rules. No reader
changes for this.

## Consequences

- A PostgreSQL-family provider engine's SQL schema documents, migration
  directives and lint input are lexed as the built-in PostgreSQL engine's
  are, with no provider call.
- The fields mirror the descriptor, so a new rule the shared lexer learns
  becomes a new field here, in a later minor.
- Rules the shared lexer does not read stay out: `line_comments` and
  `identifier_backslash_escapes` serve only the body scanner of
  `internal/deporder`, and `alternative_quoting` is a refusal on Oracle.
- An engine with a language of its own keeps `sql.scan`.
