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

**Compatibility never removes a capability. Constitute it, do not discard it.**
Where Ptah models something the community binary does not -- an extension, a
sequence, a policy, anything the Pro surface covers or that Ptah does better --
reaching CE compatibility must never mean deleting that capability from the
compatibility surface. `ptah-compat` is the migration path for Atlas
**Pro** users' scripts too, not only CE users'; a capability reachable only
through native `ptah` does not help someone porting a Pro pipeline.

The shape that satisfies both:

- the compatibility surface **defaults** to what the community binary accepts,
  so drop-in output stays drop-in;
- the fuller behavior stays reachable on that same surface behind a `PTAH_*`
  environment variable -- never a new flag, because the conformance
  `cli-surface` tier asserts flag parity with the pinned binary and an
  environment variable is invisible to the help surface. Precedent:
  `PTAH_ALLOW_EXTERNAL_SCHEMA`;
- what the default leaves out is reported, not dropped in silence, so an
  operator is never told less than the truth about their database;
- the capability is written down -- feature matrix row, user documentation, and
  a test -- so it is a product decision rather than an accident of which branch
  of an `if` ran.

"CE refuses it, so we stopped emitting it" is an incomplete answer. The complete
one names where the capability still lives.

### Compatibility with older Ptah is a different axis, and it is not owed

Everything above is about the community binary. Compatibility with **Ptah's own
previous behavior is a separate question, and until Ptah ships v1 the answer is
no.** There is no supported upgrade path to preserve, so:

- Do not keep a fallback, an alias, a tolerated old spelling, or a second reader
  for a retired format only because an earlier Ptah produced it.
- Do not soften a refusal because it would break something an earlier Ptah
  accepted.
- Do not carry a default only because changing it would alter existing output.
  Pick the default that is right for a reader meeting it for the first time.

Changing behavior is the normal, cheap thing to do right now, and the cost of
not changing it compounds. When a change alters behavior, say so plainly in the
issue and the commit -- "this changes behavior; pre-v1, so no compatibility is
owed" -- rather than quietly designing around it.

This does **not** license breaking parity with the community binary, which is a
contract with users of that CLI rather than with Ptah's own history, nor
silently discarding user data. It licenses changing Ptah's defaults, spellings,
internal formats, and error text without a migration path.

The rule expires when Ptah reaches v1.

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

### A second worked example

`file()` in an `atlas.hcl` inlines a file's contents into a config value.
Measured on the pinned community v1.3.0 build:

| argument | community binary | Ptah |
| --- | --- | --- |
| `file("local.txt")` | reads it, exit 0 | reads it, exit 0 |
| `file("/etc/passwd")` | **reads it, exit 0** | refused, exit 1 |
| `file("../../../../etc/passwd")` | **reads it, exit 0** | refused, exit 1 |
| `file("link.txt")`, a link out of the directory | **reads it, exit 0** | refused, exit 1 |

An `atlas.hcl` is repository-controlled and evaluated before anything is
applied, and the value lands somewhere observable: put the read in `env.url` and
the file's contents come back in `Error: sql/sqlclient: unknown driver "..."`.
Matching would turn config authorship into an arbitrary-file read on the machine
running the migration, which is the second half of the policy, not the first.
Ptah keeps the confinement on both binaries and names the rule in the refusal.
See [`stokaro/ptah#1042`](https://github.com/stokaro/ptah/issues/1042).

### Deciding which you are doing

Before matching a measured behavior, ask what it costs the user. If the answer
is "nothing, it is a different spelling of the same outcome", match it -- wording, exit codes,
flag names and output shape are worth being identical on, because tooling reads
them. If the answer is "they lose something they asked for", do not match it.

## Native And Compatibility Capability Ownership

`ptah-compat` is an adapter over Ptah capabilities, not an independent product
implementation.

When implementing behavior for `ptah-compat`, distinguish between:

1. a general semantic capability; and
2. Atlas-specific interface or compatibility machinery.

A general semantic capability must live in a reusable Ptah package below the
CLI layer and, where meaningful to a native Ptah user, must be reachable
through the native Ptah surface as well.

Do not implement general product behavior exclusively inside `cmd/atlas` or
another compatibility-only package.

The native surface does not need to reproduce Atlas command names, flags,
configuration syntax, output shape, URI spelling, or other interface
conventions. This is functional parity, not interface parity.

Compatibility-only adapters, parsers, codecs, persistence bridges,
diagnostics, and behavioral shims may remain compat-only when they exist
solely because of an Atlas contract.

Conversely, when native Ptah already implements a capability that has an
Atlas-compatible spelling, prefer adapting the compatibility surface to the
existing capability rather than implementing the behavior again.

CLI and compatibility packages should translate inputs and outputs, resolve
compatibility policy, and delegate semantic work to reusable
application/core packages.

The intended architecture:

```text
                         shared Ptah capabilities
                        /                        \
                       /                          \
             native Ptah surface          compatibility surface
                  `ptah`                    `ptah-compat`
```

### Which side of the boundary a change is on

These are general capabilities. They mean something without Atlas, so their
execution semantics belong in shared Ptah code with a native entry point, even
when the work that produced them was Atlas compatibility work:

```text
schema plan testing
migration testing
drift detection
schema security analysis
migration checkpoints
pre-apply checks
schema planning
schema validation
artifact publishing/fetching
migration-directory import
```

These are compatibility machinery. They exist to interpret, reproduce, or
bridge an Atlas interface or persisted representation, and they imply no native
user-facing spelling:

```text
atlas:// -> OCI resolution
Atlas CLI flag spelling and precedence
atlas.hcl compatibility parsing/evaluation
Atlas .plan.hcl codec
Atlas .test.hcl adapter
Atlas revision-table representation compatibility
Atlas checksum encoding compatibility
Atlas-compatible stdout/stderr rendering
Atlas exit-code compatibility
Atlas-specific refusal diagnostics
```

A codec feeding a shared capability is the intended shape, not a violation of
the rule:

```text
Atlas .test.hcl
      |
      v
compatibility parser
      |
      v
shared test model / runner
      |
      +--> ptah
      |
      +--> ptah-compat
```

### How this reads against the compatibility policy

The [compatibility policy](#compatibility-policy) and this rule run in opposite
directions, and neither one relaxes the other.

- The compatibility policy forbids the compatibility surface from losing a
  capability native Ptah models. A capability reachable only through `ptah` is
  no migration path for someone porting an Atlas pipeline.
- This rule forbids the native surface from losing a capability the
  compatibility surface gained. A capability reachable only through
  `ptah-compat` turns the compat tree into a second product.

They are one invariant read from two ends: neither binary is where a generally
useful capability lives, because both are adapters over the package that
implements it.

Three consequences are worth naming, because getting them backwards satisfies
this rule while breaking the older one:

- Exposing a capability natively means a **native** verb or flag. The
  compatibility surface still takes no new flag: the conformance `cli-surface`
  tier asserts flag parity with the pinned community binary, so the fuller
  behavior there stays behind a `PTAH_*` environment variable. Precedent:
  `PTAH_ALLOW_EXTERNAL_SCHEMA`.
- Reusing an existing native capability means the compatibility surface adapts
  to it. It never means narrowing the native capability to whatever the Atlas
  contract can express.
- One implementation does not force one behavior. Where the two surfaces
  deliberately diverge -- Atlas revision bookkeeping on one, recoverable
  failure state on the other -- the divergence belongs in the shared package as
  a policy the caller selects, not as a second implementation.

### Dependency direction

```text
cmd/ptah  --------------------\
                               \
                                > shared capability/application/core packages
                               /
cmd/atlas / ptah-compat ------/
```

- Native Ptah code must not depend on the compatibility command layer.
- Shared semantic packages must not depend on `cmd/atlas`.
- Atlas-specific codecs and adapters may depend on shared domain models and
  capabilities.

Conceptually:

```text
Atlas input/output contract
          |
          v
compat adapter / codec
          |
          v
shared Ptah capability
          ^
          |
native Ptah adapter
```

Today `cmd/ptah-compat/main.go` is the only non-test file outside `cmd/atlas`
that imports `cmd/atlas`; native command packages reference it from tests only.
Keep it that way.

### Classify the change in the PR

Every PR that adds or substantially extends behavior under the compatibility
surface says which of the two it is:

```text
GENERAL CAPABILITY
```

or:

```text
COMPATIBILITY ADAPTER
```

A general capability answers three questions in the PR description:

1. Where does the semantic implementation live?
2. How can native Ptah consume it?
3. If no native surface is added in the same PR, why is that reasonable, and
   what issue records the exposure gap?

A compatibility adapter names the external Atlas contract that makes it
compatibility-specific.

No GitHub PR template is required for this; `AGENTS.md` and normal PR
self-review are enough. The requirement is that the decision is made
deliberately rather than made for you by where a file happened to be placed.

### Scope of this rule

The rule is prospective. It governs work written from now on. It does not
require auditing every capability already implemented on either surface, and it
does not require refactoring packages that predate it. Add no further
divergence from here; existing architectural debt may remain for now. The
repository-wide audit belongs in a separate post-parity issue, and no such
issue is open yet. See
[`stokaro/ptah#1213`](https://github.com/stokaro/ptah/issues/1213).

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
maintainer explicitly asks for a compatibility layer. This paragraph is the
[compatibility-with-older-Ptah rule](#compatibility-with-older-ptah-is-a-different-axis-and-it-is-not-owed)
applied to code shape; the rule itself is broader and covers defaults, output,
formats and error text as well.

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

The prohibition is enforced by a `depguard` deny entry in `.golangci.yml`, so it
fires on the import declaration and is reported by `golangci-lint run ./...`
along with every other finding. It is not a text scan: a comment that ends a
sentence with the word `assert` or `require` is not a violation, and `pkg`
matches by prefix so every testify subpackage is covered by the one entry.

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
