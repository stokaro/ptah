# AGENTS.md

This file gives coding agents repository-local guidance for working in Ptah.

## Compatibility Policy

Ptah aims to be a drop-in replacement for the Atlas CLI. That goal has two
halves, and only stating the first one is how a capability gets thrown away.

**Never be looser.** A configuration or invocation the community binary refuses
must not succeed here. Accepting something it rejects means a user's mistake
passes silently on Ptah and fails somewhere later, which is the worst outcome
available. Where Ptah cannot yet implement a construct the community binary
enforces, refuse loudly rather than accept and ignore.

**Matching is the floor, not the ceiling. We do not copy defects.** Where the
community binary's behavior is a defect -- it silently drops something the
author wrote, corrupts state, or fails for a reason unrelated to what the user
asked for -- reproducing it is a wrong answer. Be the same or better. A change
whose only justification is "this is what the other implementation does" is not
justified when what it does is broken.

When the two halves pull apart, say so in the commit and in the issue rather
than picking silently. "We are stricter here, deliberately, and here is the
measurement" is a complete answer; quietly matching is not.

### A worked example

`-- atlas:txmode none` marks a migration that must run outside a transaction --
`CREATE INDEX CONCURRENTLY`, for instance. Measured on PostgreSQL 18:

| file shape | community binary | Ptah |
| --- | --- | --- |
| directive, blank line, statement | applies | applies |
| directive, statement immediately below | **fails** | applies |

The community binary requires a blank line after the directive and silently
drops it otherwise, so the statement runs inside the transaction it asked to
stay out of and the migration fails partway through. Ptah honored both forms.

A change once "fixed" this by adopting the blank-line requirement, on the
grounds that it matched. That traded a place where Ptah was better for a place
where it was merely identical, and it was reverted. The directive is honored in
both forms, and the divergence is documented rather than hidden.

### Deciding which you are doing

Before matching a measured behavior, ask what it costs the user. If the answer
is "nothing, it is a different spelling of the same outcome", match it -- wording, exit codes,
flag names and output shape are worth being identical on, because tooling reads
them. If the answer is "they lose something they asked for", do not match it.

## Language And Spelling

Use American English spelling in code, comments, documentation, issue/PR text,
and user-facing CLI output unless preserving an exact external quote or protocol
token. Prefer spellings such as `behavior`, `color`, `canceled`, `initialize`,
`normalize`, and `analyze`.

## Documentation Obligations

All documentation work must follow the authoritative style guide at
[`docs/STYLE_GUIDE.md`](docs/STYLE_GUIDE.md): classify the page type before
writing, use the matching template, keep the canonical terminology, and run
the style guide's review checklist. When a reader-facing page is added, moved,
merged, split, or retired, update the content inventory at
[`docs/site/CONTENT_INVENTORY.md`](docs/site/CONTENT_INVENTORY.md) in the same
PR.

CI enforces the mechanical half of that guide. Section 13 of the guide lists
exactly which rules fail a build and which stay a reading responsibility. Run
`node docs/site/scripts/check-style.mjs` for any documentation change: it needs
no npm install, and it governs this file, the repository docs, the examples,
the integration docs, and every package README — not only the site.

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
- Atlas-compatible behavior in the `ptah-compat` drop-in binary.
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
aligned with canonical Ptah command paths. Atlas OSS command parity lives only
in the separate `ptah-compat <command> ...` drop-in binary at process root;
the native `ptah` binary has no Atlas command paths. Do not document
root-level Atlas aliases inside the native `ptah` binary such as
`ptah migrate apply`. Do not claim full Atlas parity unless the current
conformance evidence proves it.

For deep documentation maintenance, use the repo-local skill at
`.agents/skills/ptah-documentation-maintenance/SKILL.md`. It is Ptah-specific:
it routes CLI, config, migration, parser/renderer, conformance, public API, and
example changes to the right documentation surfaces and uses Inventario's docs
site as the quality reference.

## Code Style And Linting

Ptah treats `.golangci.yml` as a strict contract. Fix code to satisfy the configured linters instead of relaxing thresholds, disabling checks, or broadening exclusions. In particular, keep `revive` `error-strings` enabled and preserve the current "stricter wins" lint posture unless a maintainer explicitly asks for a config change.

Ptah is pre-GA. Do not preserve old command aliases, compatibility wrappers,
fallback APIs, or backward-compatibility behavior only to keep an older internal
shape. Prefer the cleaner architecture and update callers/tests/docs unless a
maintainer explicitly asks for a compatibility layer.

Atlas OSS command parity belongs in the separate `ptah-compat` binary, the
Atlas-style root command surface for drop-in script migration. The `ptah`
binary is purely native. Do not add Atlas command spellings or temporary
aliases such as `ptah migrate apply` or a `ptah atlas` namespace to the `ptah`
binary; remove or redesign old native paths instead of preserving them.

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

### Package Documentation

Every Go package must carry a package-level doc comment (`// Package <name>
...`), either atop a central file of the package or in a dedicated `doc.go`.
This is CI-enforced through staticcheck's `ST1000` in `.golangci.yml`; a PR
that introduces a new package must ship the comment in the same PR. The rule
applies to every module in the repository, including `testkit/`. `main`
packages describe their binary; test-only packages (`package foo_test`) are
exempt.

The comment must say in one to three sentences what the package does and where
it sits in the system, grounded in the package's actual code. Generic filler
such as "Package x contains x utilities" is not acceptable — the anti-slop
rules of [`docs/STYLE_GUIDE.md`](docs/STYLE_GUIDE.md) apply in spirit.

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
scripts/check-test-style.sh --write-baseline
```

Regenerate through the script, not through `go tool teststyle -write-baseline`
directly: the bare tool walks the filesystem and cannot tell a linked git
worktree parked under the repository from the repository itself, so it records
tests that are not in the working tree.

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

	"go.5x5.cz/ptah/internal/atlasurl"
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
