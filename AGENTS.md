# AGENTS.md

This file gives coding agents repository-local guidance for working in Ptah.

## Language And Spelling

Use American English spelling in code, comments, documentation, issue/PR text,
and user-facing CLI output unless preserving an exact external quote or protocol
token. Prefer spellings such as `behavior`, `color`, `canceled`, `initialize`,
`normalize`, and `analyze`.

## Documentation Obligations

Before finishing any change that affects external behavior, inspect and update
the relevant documentation. Do this as a required verification step, not as an
opportunistic cleanup. Purely code-internal refactors that do not alter public
behavior, user-facing output, generated artifacts, supported inputs, or
operational workflows may skip documentation edits, but the self-review should
still confirm that the change is internal-only.

External behavior includes at least:

- CLI command names, command grouping, flags, environment variables, help
  output, output formats, and exit codes.
- Config file formats, accepted keys, validation behavior, environment
  selection, and precedence rules.
- Generated SQL, parsed SQL, migration file formats, migration directives,
  revision table behavior, hash files, and validation/repair semantics.
- Public Go package APIs and any documented extension points.
- Atlas-compatible behavior under `ptah atlas <command> ...`.
- Conformance status, supported/unsupported feature claims, known gaps, and
  documented limitations.
- User-facing errors, warnings, diagnostics, logs, safety checks, and failure
  behavior.

When a change touches any of those areas, build a documentation impact map and
search the relevant `.md` files before considering the task complete. Check at
least:

- `README.md`.
- `docs/README.md`, `docs/*.md`, and the task-oriented docs under
  `docs/site/src/content/docs/`.
- `examples/**/README.md` and generated example artifacts when examples change.
- `integration/*.md` and test-runner docs when test, fixture, or database
  behavior changes.
- Package-level READMEs such as `internal/parser/README.md`,
  `migration/generator/README.md`, and `migration/migrator/README.md` when the
  corresponding package behavior changes.
- `AGENTS.md` itself when agent workflow or project rules change.

Search for both old and new terms: command names, aliases, flag names,
environment variables, config keys, issue numbers, dialect names, conformance
gap names, generated labels, and exact error strings. Documentation must stay
aligned with canonical Ptah command paths, especially the rule that Atlas OSS
command parity belongs only under `ptah atlas <command> ...`; do not document
root-level Atlas aliases. Do not claim full Atlas parity or drop-in replacement
status unless the current conformance evidence proves it.

## Code Style And Linting

Ptah treats `.golangci.yml` as a strict contract. Fix code to satisfy the configured linters instead of relaxing thresholds, disabling checks, or broadening exclusions. In particular, keep `revive` `error-strings` enabled and preserve the current "stricter wins" lint posture unless a maintainer explicitly asks for a config change.

Ptah is pre-GA. Do not preserve old command aliases, compatibility wrappers,
fallback APIs, or backward-compatibility behavior just to keep an older internal
shape. Prefer the cleaner architecture and update callers/tests/docs unless a
maintainer explicitly asks for a compatibility layer.

Atlas OSS command parity belongs under `ptah atlas <command> ...` only. Do not
add root-level Atlas command spellings or temporary aliases such as
`ptah migrate apply` / `ptah schema inspect`; remove or redesign old paths
instead of preserving them.

The `modernize` linter is enabled. Prefer current Go idioms when writing or editing code:

- Use standard library helpers such as `slices.Contains`, `maps.Copy`, `strings.CutPrefix`, and `strings.SplitSeq` when they fit the code.
- Use `any` instead of `interface{}`.
- Do not add pointer helper packages or local `stringPtr`/`strPtr` helpers for new code; follow the idioms accepted by `modernize`.
- Use `fmt.Fprintf(&builder, ...)` rather than `builder.WriteString(fmt.Sprintf(...))`.
- Prefer clear early returns and simple control flow that satisfies `revive`, `gocognit`, `gocyclo`, `nestif`, and `funlen`.
- Keep import aliases compliant with `importas`; for example, `github.com/frankban/quicktest` must be imported as `qt`.
- Add `//nolint` only when necessary, always with a specific linter name and an explanation.

When applying automatic lint fixes, run both passes:

```bash
golangci-lint run --fix ./...
golangci-lint run ./...
```

The fix pass can leave second-pass fallout such as unused imports, removed helper functions, or staticcheck suggestions. Clean those manually before considering the lint run complete.
