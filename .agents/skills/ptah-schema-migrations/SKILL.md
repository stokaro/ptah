---
name: ptah-schema-migrations
description: Use when changing Ptah SQL parsing, schema sources, AST or model conversion, inspection, comparison, planning, rendering, or migration execution. Apply to fixes that can lose schema information, alter SQL order, or affect database state.
---

# Ptah schema and migrations

## Trace one shipping path

Identify the input, target, selected runtime, and observable failure. Trace the
actual command or public API through the affected boundaries before editing.
Use [architecture policy](../../references/architecture.md) to locate the
packages, then read only the relevant source and decision sections.

| Concern | Entry points and contracts |
| --- | --- |
| Source declarations | `core/goschema`, `core/yamlschema`, `core/schemasource` |
| SQL syntax and owner claims | `internal/parser`, `internal/sqlschema`, `core/sqlext` |
| Identity and knowledge | `core/objectidentity`, `core/schemaext`, `core/schemamodel`, `catalog` |
| Comparison and planning | `migration/schemadiff`, `core/difftypes`, `migration/planner`, `core/planext` |
| Selected services | `engine`, `engine/builtin`, `dialect/`, `feature/` |
| Execution and durable state | `migration/migrator`, `migration/shadow`, `migration/migrationfile` |

Do not infer support from an accepted input. Read descriptors and capabilities
for the selected target. Provider-owned engines need the provider-contract
skill; a built-in-only fix must not become an assumption in shared code.

## Preserve information and ownership

- Trace each changed field or object through parse, model, conversion,
  comparison, planning, rendering, and readback where those stages apply.
  Check copies and serializers as well as constructors. Preserve owner data,
  identity, coverage, and diagnostics together.
- Treat uninspected, unrepresentable, defaulted, and explicitly absent state
  as different facts. Never turn incomplete inspection into removal intent.
  `engine/comparison_coverage.go` enforces this at the comparison boundary.
- Reuse `core/objectidentity` and the target's identifier semantics. Preserve
  schema, parent, kind, quotedness, and overload identity. Do not build keys
  by joining names or normalize identifiers with generic string functions.
- Keep engine semantics with their owner. Neutral layers and the shared SQL
  reader must not gain concrete owner imports. Read the applicable boundary
  in `docs/architecture_boundaries.md` before changing dependency direction.
- Reuse the shared lexer/parser and `core/sqlext` claims. A lexical statement
  splitter is not a procedural parser. Test quotes, comments, nested syntax,
  malformed input, and source positions when the changed grammar reaches them.
- Preserve deterministic bytes and dependency order separately. Sorting a
  plan to stabilize output can make executable SQL invalid. A runtime's
  selected factories must not change another runtime's behavior.

## Review execution effects

For changes that reach a database, inspect transaction and connection scope,
locks, dry-run behavior, revision and log records, partial success, rollback,
and cleanup. Select the relevant paths; do not expand a parser-only fix into
an unrelated execution refactor.

Preserve `ptaherr.ErrOutcomeUnknown`: a failed call may already have changed
durable state. Never infer that replay is safe from a connection error or add
an automatic retry without the existing recovery contract. See
`migration/migrator/connection_failure_outcome_test.go` for the applied-then-
failed case and the refusal to replay an uncertain statement.

## Select discriminating evidence

Start with a regression through the real entry point and show it fails without
the fix. Choose additional cases from the changed contract:

- Coverage: incomplete inspection preserves an object; complete explicit
  absence still removes it. Use
  `migration/schemadiff/coverage_removal_invariant_test.go` as the control pattern.
- Identity: same names in different schemas, quoted names, delimiters inside
  names, or target-specific folding must remain distinguishable as appropriate.
- Determinism: equivalent inputs with reversed insertion order must agree;
  repeated runs alone may agree by chance. See
  `migration/schemadiff/determinism_invariant_test.go`.
- Pipeline: assert the resulting semantic diff or plan as well as a parser
  node when the defect involved lost information between stages.
- Execution: assert state read back from the engine, including failure and
  cleanup state where relevant. An expected SQL string alone cannot establish
  acceptance, transaction behavior, permissions, or catalog semantics.

Apply the engine-level test conditions and provider ownership rules in
[testing policy](../../references/testing.md). Keep the focused unit regression;
add live or command evidence when the engine or process decides the outcome.
Use the validation policy to bound local iteration without weakening the
required evidence. State which engine/version and path were actually exercised.
