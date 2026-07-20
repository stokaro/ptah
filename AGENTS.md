# AGENTS.md

This file gives coding agents repository-local guidance for working in Ptah.

## Language And Spelling

Use American English spelling in code, comments, documentation, issue/PR text,
and user-facing CLI output unless preserving an exact external quote or protocol
token. Prefer spellings such as `behavior`, `color`, `canceled`, `initialize`,
`normalize`, and `analyze`.

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
