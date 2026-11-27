# Go implementation policy

Repository-specific requirements for the matching task. Paths in code spans
are relative to the repository root. Read only the sections the task needs.

## Contents

- [Code Style And Linting](#code-style-and-linting)
- [Public API doc comments and examples](#public-api-doc-comments-and-examples)
- [Recognition that spans two functions belongs to one of them](#recognition-that-spans-two-functions-belongs-to-one-of-them)
- [A path is not a string, and an assertion about one is not portable](#a-path-is-not-a-string-and-an-assertion-about-one-is-not-portable)
- [A handle is released by a caller, not by the collector](#a-handle-is-released-by-a-caller-not-by-the-collector)
- [Language And Spelling](#language-and-spelling)
- [Say what holds now, not when it changed](#say-what-holds-now-not-when-it-changed)

## Code Style And Linting

- `.golangci.yml` is a strict contract: fix code, never relax thresholds or
  widen exclusions. Use focused lint feedback while editing. Apply automatic
  fixes only to the intended scope and inspect the diff. Complete evidence
  includes both default and discovered-tag contours (`make lint-golangci`),
  locally or in the applicable CI job. Every discovered tag except `integration`
  is built into `ptah` and tested in the `build-tags` job of
  `go-unit-tests.yml`.
- `modernize` is on (`slices.Contains`, `maps.Copy`, `strings.CutPrefix`,
  `any`, `fmt.Fprintf(&b, ...)`, `new(expr)` over a named temporary); no
  pointer helper packages.
- Suppressions name the rule and a reason (`#nosec Gxxx -- reason`,
  `//revive:disable... reason`), never a bare `//nolint:gosec`; `nolintguard`
  runs through `go vet -vettool` in both contours.
- A closing parenthesis goes on its own line when the arguments began on
  their own lines; no linter checks it. Re-read the diff after `gofmt -r`.
- Discover the module list, never write it: `scripts/list-go-modules.sh`
  (from `git ls-files`) feeds `make lint`, and
  `scripts/check-go-module-lint-coverage.sh` fails when a lint job misses a
  module or passes a tag list other than the discovered one. Any "every
  module" claim needs discovery or a check.
- Every package has a `// Package <name>` comment, grounded in the code, on
  what it does and where it sits (`ST1000`); no filler.

### Public API doc comments and examples

- Embedders read ledger packages through godoc without this repository, so
  doc comments state error semantics and sentinels, zero-value and nil
  behavior, ordering and determinism, mutation versus copy, accepted dialects.
- **Document stable caller-observable behavior, not incidental implementation
  behavior.** Generalize a sentence a compatible internal change would
  falsify; state an intended guarantee precisely.
  `scripts/check-exported-docs.sh` checks presence only.
- Important usage patterns get executable examples in `example_test.go`,
  shaped like `core/astbuilder/example_test.go`: black-box, attached to a
  symbol by name, deterministic `// Output:` from offline fixtures, a doc
  comment on what it shows. Not one `ExampleFoo` per symbol. A compile-only
  example is the fallback when output cannot be deterministic, and says why.
- Examples keep `if err != nil`, and `scripts/check-test-style.sh` skips them.
  `must.Must` (`github.com/go-extras/go-kit/must`) is for fixture setup, never
  for the demonstrated call.

### Recognition that spans two functions belongs to one of them

Two functions that must recognize the same set share one predicate, whose
declaration says why it cannot split. Two lists drift when one is extended;
the control drives the public path that joins them.

### A path is not a string, and an assertion about one is not portable

These pass on Linux and fail on `windows-latest`:

1. A path in HCL, YAML or a Go template (`\U` is an escape): use
   `filepath.ToSlash` or `strconv.Quote`.
2. `url.URL.Path` through `String()` (`%5C`, `//` before a drive letter): use
   `atlasurl.SQLiteURLFromPath`.
3. Text the OS wrote: assert with `errors.Is` or on Ptah's part only;
   `testutils.StatMissingText` and `syscall.ENOENT.Error()` derive the OS
   clause.
4. `go build -o dir/tool` is not executable there: use `internal/exeext`.
5. `os.PathError.Op` is `stat` on Unix and `GetFileAttributesEx` on Windows.
6. Shell grammar in a fixture (`echo m; exit 9`): use `internal/preflight` or
   `testutils.FailingHookCommand`.
7. Win32 refuses `< > : " / \ | ? *` in a file name: split the test by
   concern, not platform, with a portable name for the round trip and the
   escaping asserted as a string everywhere.
8. `os/exec` does not set `PWD` on Windows; Ptah's contract does, so Ptah
   sets it.

- **A platform-conditional assertion tends to pass on the platform it cannot
  test** (a mode check becomes `0o200 == 0o200`): pass the varying part in or
  split by build tag, never leave a tautology on the half you are not looking
  at.
- **A wildcard is not a wildcard across lines**: `qt.ErrorMatches` anchors the
  message and `.` skips newlines, which Windows messages often hold. Assert
  the sentinel with `qt.ErrorIs` and pin a prefix with `(?s)`.
- A rule about file names must not depend on the host: `filepath.IsAbs` calls
  `/tmp/x` relative on Windows, so `atlas.hcl` confinement refuses a leading
  slash, backslash or drive letter everywhere.

### A handle is released by a caller, not by the collector

A type holding an OS handle offers `Close`; on Windows an open directory
handle blocks removal until the collector runs. Releasing twice, or after the
handle is gone, is a no-op, so `defer` beside the constructor is always
right. Check the production abandonment path, not only tests, and say which
release happened. Test it white-box on the handle field, and say in the file
why a black-box test cannot exist.


## Language And Spelling

- American English in code, comments, docs, issues, PRs and CLI output,
  except exact external quotes and protocol tokens.
- **Plain international English in everything you write**, including commits,
  PR and issue text, comments, CLI output and review replies: short
  sentences, concrete nouns, active voice, one idea per sentence, for readers
  who are often not native speakers. Say what changed and why, once.
  [`docs/STYLE_GUIDE.md`](../../docs/STYLE_GUIDE.md) is the authority (sections 4
  to 6).
- Avoid the default assistant register: marketing adjectives and filler
  intensifiers; `Certainly`, `Great question`, `I hope this helps`;
  `delve into`, `leverage`, `utilize`; `not just X, but Y`; rule-of-three
  flourishes; a rhetorical question opening a section; a closing paragraph
  that restates; throat-clearing. Do not narrate the diff: give the reason,
  trade-off or risk.
- **A count is not a subject**: never `Two things follow:`, `Decide four
  things` or `asserts three things`, nor the same with a nicer noun. A
  lead-in says what the items share, or it is dropped; prose names the noun,
  Go doc comments included. `one thing` and `two different things` are fine.
  Style guide 5.3; `internal/countsubjectguard` and `check-style.mjs` enforce
  it.

### Say what holds now, not when it changed

- **Never date a statement to a Ptah issue or narrate what Ptah did before**,
  in pages or comments. Write what is still true: the ablation (`without this
  call every write wins`) or a measurement without its calendar. Cite an
  issue that still owns something plainly: `(stokaro/ptah#2209)` is a
  pointer, `since stokaro/ptah#2209` is a date.
- Read the subject of `used to`, `no longer`, `previously` or `originally`:
  behind a pronoun or `Ptah` it is history; behind a thing (`the rows used to
  derive it`, `a row the source no longer has`) it is current and stays.
- `check-implementation-chronology.mjs` (Markdown) and
  `internal/chronologyguard` (Go comments) catch the fixed shapes; the rest is
  yours, and a forward reference to owned work (`after stokaro/ptah#2725
  removes the converter`) is allowed. Style guide 6.7 has the full rule.
