# sqlsyntax

`github.com/stokaro/ptah/sqlsyntax` holds the lexical syntax of SQL families that Ptah and its providers read alike. It is released apart from Ptah, as `provider-sdk` is, with tags named `sqlsyntax/vX.Y.Z`, so a provider depends on the syntax without depending on the host module.

It knows no engine's catalog, types or DDL. Reading, rendering and planning stay with the engine that owns them.

## pgsyntax

Package `pgsyntax` reads PostgreSQL-family text:

- `Scan` splits text into tokens: white space, line and nested block comments, words, quoted identifiers (`"..."`, `U&"..."`), strings (`'...'`, `E'...'`, `U&'...'`, `N'...'`, `B'...'`, `X'...'`), dollar-quoted strings, numbers, positional parameters, operators and punctuation. Every byte belongs to one token.
- `Next` reads the first token of a text, for a reader that keeps its own scanner and reads one construct by these rules.
- `Statements` splits text at semicolons outside strings, identifiers, dollar quotes and comments.
- `Value` reads what a string constant or quoted identifier stands for: its escapes, the segments that continue a string on later lines, and the `UESCAPE` clause of a Unicode escape string. It reports no value where the server refuses the constant.
- `QuoteIdentifier` and `QuoteString` write a name and a value so that a server reads them back exactly.

`Options{}` is PostgreSQL's default, with `standard_conforming_strings` on. `Options{BackslashEscapes: true}` reads a backslash in an ordinary string as an escape, as a server with that setting off does.

```go
statements, err := pgsyntax.Statements(body, pgsyntax.Options{})
```

The split is lexical: the body of a function written with `BEGIN ATOMIC` is split at its semicolons, which the server reads as part of the function.
