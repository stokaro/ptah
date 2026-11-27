# Repository architecture

Repository-specific requirements for the matching task. Paths in code spans
are relative to the repository root. Read only the sections the task needs.

## What Ptah Is

Ptah generates SQL DDL from annotated Go structs, compares a desired schema
with a live database, and plans, generates and applies migrations. It ships
`ptah` and `ptah-compat`, a drop-in replacement for the Atlas CLI. Read facts
from their declarations, not from counts here:

- Dialects: the descriptors in `internal/builtindialects` feed
  `core/platform.Dialects` and `NormalizeDialect`. Shared code reads a
  descriptor field, never a dialect name (stokaro/ptah#4204). `migrations
  lint` and `sql lint` skip Oracle on purpose (`internal/lintdialect/dialect.go`).
  DuckDB, Spanner and YDB are served by providers installed separately (ADR
  0031), and their engine knowledge lives in the provider (ADR 0023).
- Drivers: the descriptor's `Driver`, read by
  `internal/connector/<engine>connect`; a `sqlite` URL may take `libsql`
  (`internal/connector/sqliteconnect`).
- Rendering: `docs/capabilities.md`. Accepted is not promised: `SERIAL` is
  refused by name on ClickHouse, never downgraded.

## Repository Layout

- Most code is under `internal/`; check where a package lives before writing
  an import path.
- The public Go surface is the ledger [`docs/public_api.md`](../../docs/public_api.md).
  `scripts/check-public-api.sh` classifies every importable library package as
  Stable Embedder API or Documentation-Only.
  `scripts/check-public-api-released.sh` diffs against the newest tag (an
  intentional break needs a reviewed entry in `docs/public_api_approvals.txt`),
  and `scripts/check-exported-docs.sh` requires every exported declaration to
  be documented. Those two read only the stable category, because a
  documentation-only package carries no guarantee.
- "Library package" is package metadata: `main` packages, `_test.go`-only
  directories and anything behind an `internal/` path segment (`core/internal`,
  not `internalized`) are outside. The gate has no exemptions and gets none;
  move the subtree instead.
- Additive API changes get normal review; never commit a generated snapshot of
  exported declarations.
- Core packages: `core/ast`, `core/astbuilder`, `core/goschema` (annotation
  parser in `parser.go`), `engine/builtin` (dialects in
  `engine/builtin/internal/dialects/`), `core/platform`, `core/yamlschema`,
  `dbschema`, `migration/generator`, `migration/migrationfile`,
  `migration/shadow`, `migration/migrator`, `migration/planner`,
  `migration/schemadiff`.
- `cmd/` holds only shipped programs. `internal/cli/root/root.go` assembles
  `ptah` from the namespaces under `internal/cli/`, one package per leaf verb
  (`internal/cli/generate` backs `ptah schema render`); `internal/cli/atlas`
  is the tree `cmd/ptah-compat` ships. Shared helpers sit in
  `internal/cli/internal/`, except `internal/cli/banner` and
  `internal/cli/cliobs`, which a `main` package imports. `cmd/integration-test`
  runs the suite; `cmd/ptah-ls` is the language server.
- Test entities: `internal/stubs/`, `examples/`,
  `integration/internal/fixtures/entities/`.

## Schema Annotations

- Directives are `//ptah:` comments on Go structs; see `internal/stubs/`.
- **An index annotation has to sit on a struct field**; at file level it is
  silently ignored. For an index away from its column, annotate a holder
  struct's `_ int` field and name the table:
  `//ptah:schema:index name="..." fields="name" table="products"`.
- `fields=` is current and `columns=` a legacy synonym; unknown attributes
  and unknown directive names on a struct or a field fail at parse time.
