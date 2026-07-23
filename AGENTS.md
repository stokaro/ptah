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
aligned with canonical Ptah command paths. Atlas OSS command parity exists in
two product surfaces: `ptah atlas <command> ...` inside the full-featured `ptah`
CLI, and the separate `ptah-compat <command> ...` drop-in binary at process
root. Do not document root-level Atlas aliases inside the native `ptah` binary
such as `ptah migrate apply` or `ptah schema inspect`. Do not claim full Atlas
parity unless the current conformance evidence proves it.

For deep documentation maintenance, use the repo-local skill at
`.agents/skills/ptah-documentation-maintenance/SKILL.md`. It is Ptah-specific:
it routes CLI, config, migration, parser/renderer, conformance, public API, and
example changes to the right documentation surfaces and uses Inventario's docs
site as the quality reference.

## Code Style And Linting

Ptah treats `.golangci.yml` as a strict contract. Fix code to satisfy the configured linters instead of relaxing thresholds, disabling checks, or broadening exclusions. In particular, keep `revive` `error-strings` enabled and preserve the current "stricter wins" lint posture unless a maintainer explicitly asks for a config change.

Ptah is pre-GA. Do not preserve old command aliases, compatibility wrappers,
fallback APIs, or backward-compatibility behavior just to keep an older internal
shape. Prefer the cleaner architecture and update callers/tests/docs unless a
maintainer explicitly asks for a compatibility layer.

Atlas OSS command parity belongs under `ptah atlas <command> ...` inside the
full-featured `ptah` binary. The separate `ptah-compat` binary is the
Atlas-style root command surface for drop-in script migration. Do not add
root-level Atlas command spellings or temporary aliases such as
`ptah migrate apply` / `ptah schema inspect` to the `ptah` binary; remove or
redesign old native paths instead of preserving them.

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

## Testing Standards

### Declarative Tests Only

All tests MUST be purely declarative. The following are prohibited in test
functions:

- `if` statements.
- `switch` statements.
- `goto` statements.

`for` loops are allowed in test functions for table-driven tests that iterate
over a static list of cases, and are not considered conditional logic for this
guideline. Keep loop bodies simple and do not use loops to encode branching
logic.

Go 1.22 and newer makes range variables per-iteration, so the historical
`test := test` workaround is not needed when using `c.Run()` closures in
table-driven tests unless intentionally taking the address of a loop variable.

Run the test-style baseline before finishing test changes:

```bash
scripts/check-test-style.sh
```

The baseline records existing violations while issue #541 is being cleaned up.
New tests must not add entries. Cleanup PRs that intentionally remove entries
should refresh the baseline with:

```bash
GOWORK=off go run ./internal/tools/teststyle -write-baseline
```

Never use `testify` in Ptah code, tests, examples, or documentation snippets.
Use `quicktest` imported as `qt`, the Go standard library `testing` package, or
existing project-specific test helpers instead. Existing transitive dependency
metadata from third-party packages is not permission to add direct
`github.com/stretchr/testify` imports or `assert`/`require` examples.

Bad:

```go
func TestDialectFromURL(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		rawURL  string
		want    string
		wantErr string
	}{
		{name: "postgres", rawURL: "postgres://localhost/dev", want: "postgres"},
		{name: "unsupported", rawURL: "spanner://localhost/dev", wantErr: `unsupported --dev-url dialect "spanner://localhost/dev"`},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got, err := atlasurl.DialectFromURL(test.rawURL)
			if test.wantErr != "" {
				c.Assert(err, qt.ErrorMatches, test.wantErr)
				return
			}
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}
```

Good:

```go
func TestDialectFromURL_HappyPath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		rawURL string
		want   string
	}{
		{name: "postgres", rawURL: "postgres://localhost/dev", want: "postgres"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got, err := atlasurl.DialectFromURL(test.rawURL)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestDialectFromURL_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("unsupported", func(c *qt.C) {
		got, err := atlasurl.DialectFromURL("spanner://localhost/dev")
		c.Assert(err, qt.ErrorMatches, `unsupported --dev-url dialect "spanner://localhost/dev"`)
		c.Assert(got, qt.Equals, "")
	})
}
```

### Do Not Hide Conditionals In Helpers

Avoid helper functions that mask conditional logic, such as choosing between
`qt.ErrorIs`, `qt.ErrorMatches`, and `qt.IsNil` based on fields in a test case.
This makes tests harder to read and review.

Instead, write explicit assertions per case, even when it is a bit repetitive.

Bad:

```go
func checkError(c *qt.C, err error, wantIs error, wantLike string) {
	if wantIs != nil {
		c.Check(err, qt.ErrorIs, wantIs)
		return
	}
	if wantLike != "" {
		c.Check(err, qt.ErrorMatches, wantLike)
		return
	}
	c.Check(err, qt.IsNil)
}
```

Good:

```go
c.Run("unsupported dev url dialect", func(c *qt.C) {
	got, err := atlasurl.DialectFromURL("spanner://localhost/dev")
	c.Assert(err, qt.ErrorMatches, `unsupported --dev-url dialect "spanner://localhost/dev"`)
	c.Assert(got, qt.Equals, "")
})

c.Run("postgres dev url", func(c *qt.C) {
	got, err := atlasurl.DialectFromURL("postgres://localhost/dev")
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "postgres")
})
```

### Separate Happy-Path And Failure-Path Tests

Do not mix success and error cases in the same table. Prefer either:

- `TestXxx_HappyPath` and `TestXxx_FailurePath`.
- Separate `c.Run("happy ...")` and `c.Run("failure ...")` groups with distinct
  tables.

Bad:

```go
tests := []struct {
	name    string
	rawURL  string
	want    string
	wantErr string
}{
	{name: "postgres", rawURL: "postgres://localhost/dev", want: "postgres"},
	{name: "unsupported", rawURL: "spanner://localhost/dev", wantErr: `unsupported --dev-url dialect "spanner://localhost/dev"`},
}

for _, test := range tests {
	c.Run(test.name, func(c *qt.C) {
		got, err := atlasurl.DialectFromURL(test.rawURL)
		if test.wantErr != "" {
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			return
		}
		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.Equals, test.want)
	})
}
```

Good:

Use table-driven tests with `c.Run()` for multiple test cases:

```go
func TestDialectFromURL_HappyPath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		rawURL string
		want   string
	}{
		{name: "postgres", rawURL: "postgres://localhost/dev", want: "postgres"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got, err := atlasurl.DialectFromURL(test.rawURL)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestDialectFromURL_FailurePath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name    string
		rawURL  string
		wantErr string
	}{
		{
			name:    "unsupported",
			rawURL:  "spanner://localhost/dev",
			wantErr: `unsupported --dev-url dialect "spanner://localhost/dev"`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got, err := atlasurl.DialectFromURL(test.rawURL)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.Equals, "")
		})
	}
}
```

Error checking patterns:

```go
// Success case.
c.Assert(err, qt.IsNil)

// Preferred for sentinel errors because it handles wrapped errors.
c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidConfig)

// Error type checks.
var pathErr *os.PathError
c.Assert(err, qt.ErrorAs, &pathErr)

// Regex match when no sentinel is available.
c.Assert(err, qt.ErrorMatches, "failed to load schema.*")

// Substring check when matching part of the message is clearer.
c.Assert(err, qt.IsNotNil)
c.Assert(err.Error(), qt.Contains, "connection refused")
```

### Black-Box Testing By Default

By default, all Go tests use black-box testing:

- Test file: `*_test.go`.
- Package name: `package atlasurl_test` with the `_test` suffix.
- Test only exported API.

Bad:

```go
package atlasurl

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestDialectFromURL_HappyPath(t *testing.T) {
	c := qt.New(t)
	got, err := DialectFromURL("postgres://localhost/dev")
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "postgres")
}
```

Good:

```go
package atlasurl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasurl"
)

func TestDialectFromURL_HappyPath(t *testing.T) {
	c := qt.New(t)
	got, err := atlasurl.DialectFromURL("postgres://localhost/dev")
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "postgres")
}
```

### White-Box Testing As An Exception

White-box testing, meaning same-package tests with access to unexported symbols,
is permitted only when:

1. Testing unexported functions critical for correctness.
2. Testing internal state that cannot be observed through exported API.
3. There is a clear technical justification.

Requirements for white-box tests:

- File naming: `*_internal_test.go`.
- Package name: `package parser` without the `_test` suffix.
- Include a `// White-box testing required:` comment as the first non-empty line
  after the `package` line explaining the justification.

Bad:

```go
package parser

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func Test_cursor(t *testing.T) {
	c := qt.New(t)
	cursor := newCursor("CREATE TABLE users (id BIGINT);")
	c.Assert(cursor.peek(), qt.Equals, "CREATE")
}
```

Good:

```go
package parser

// White-box testing required: this file verifies parser cursor invariants that
// are not observable through the exported Parse API without making assertions
// dependent on renderer output.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)
```
