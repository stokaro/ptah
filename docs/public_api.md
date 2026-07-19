# Public Go API

Ptah is pre-GA, so this document defines the intended public API surface before
the first stable release. Public packages should return typed errors instead of
requiring callers to match strings.

## Typed Errors

Use `github.com/stokaro/ptah/core/ptaherr` with standard Go error matching:

```go
var parseErr *ptaherr.ParseError
if errors.As(err, &parseErr) {
    // parseErr.File, parseErr.Line, parseErr.Directive, parseErr.Attribute
}

if errors.Is(err, ptaherr.ErrUnsupportedDialect) {
    // choose a supported dialect or report configuration feedback
}
```

The first typed error contract covers:

- `*ptaherr.ParseError` from Go schema parsing failures.
- `*ptaherr.RenderError` from renderer selection and AST rendering failures.
- `*ptaherr.PlanError` from planner selection and migration planning failures.
- `ptaherr.ErrUnknownAttribute`, `ErrMissingRequiredAttribute`,
  `ErrInvalidAttributeValue`, `ErrUnsupportedDialect`, and
  `ErrUnsupportedFeature` sentinels.

## Supported Packages

These packages are intended for embedders:

- `core/ast`
- `core/goschema`
- `core/platform`
- `core/platform/capability`
- `core/ptaherr`
- `core/renderer`
- `core/sqlutil`
- `dbschema`
- `migration/generator`
- `migration/lint`
- `migration/migrator`
- `migration/planner`
- `migration/risk`
- `migration/safety`
- `migration/schemadiff`
- `migration/seeder`

Packages under `cmd/`, `internal/`, and nested implementation packages such as
renderer and planner dialect packages are not part of the supported API unless a
future release promotes them here.

## Remaining Pre-GA Work

Before a stable release, Ptah still needs an automated exported-surface check
against this document and a cleanup pass that moves unlisted implementation
packages under `internal/` or explicitly documents why they remain importable.
