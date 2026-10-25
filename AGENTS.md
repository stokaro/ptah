# AGENTS.md

Rules for coding agents in Ptah that no gate enforces. Where a gate or another
document holds a rule, this file points to it, because a copy drifts.

## Where Work Happens

**Work only in a linked git worktree, never in the main one.** Two agents work
here at once, and a branch checked out in the main worktree moves the other
agent's work.

```bash
git fetch origin
git worktree add .claude/worktrees/<name> -b <branch> origin/master
git worktree add .codex/worktrees/<name>  -b <branch> origin/master
```

- Name the worktree for the task (`2538-constraint-include`, not `tmp`), and
  branch from `origin/master`, never from a local `master`.
- After the merge, run `git worktree remove <path>` and `git branch -D`. `-d`
  does not see a squash merge, so first confirm the content reached master.
- **Never remove a worktree you did not create**: it is another agent's claim,
  and `--force` destroys work with no pull request yet.
- A gate that enumerates files asks `git ls-files`, never the filesystem,
  which also holds parked checkouts (as `scripts/check-test-style.sh` and
  `scripts/list-go-modules.sh` do). A gate that regenerates requires every
  generator to exit 0, and one that enumerates holds its corpus above a floor;
  otherwise it reports without running.

## What Ptah Is

Ptah generates SQL DDL from annotated Go structs, compares a desired schema
with a live database, and plans, generates and applies migrations. It ships
`ptah` and `ptah-compat`, a drop-in replacement for the Atlas CLI. Read facts
from their declarations, not from counts here:

- Dialects: the descriptors in `internal/builtindialects` feed
  `core/platform.Dialects` and `NormalizeDialect`. Shared code reads a
  descriptor field, never a dialect name (stokaro/ptah#4204). `migrations
  lint` and `sql lint` skip Oracle on purpose (`internal/lintdialect/dialect.go`).
  YDB is supported end to end, including YQL lint, data writes and embedding
  generations.
- Drivers: the descriptor's `Driver`, read by
  `internal/connector/<engine>connect`; a `sqlite` URL may take `libsql`
  (`internal/connector/sqliteconnect`).
- Rendering: `docs/capabilities.md`. Accepted is not promised: `SERIAL` is
  refused by name on ClickHouse, CockroachDB and Spanner, never downgraded.

## Repository Layout

- Most code is under `internal/`; check where a package lives before writing
  an import path.
- The public Go surface is the ledger [`docs/public_api.md`](docs/public_api.md).
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
  parser in `parser.go`), `engine/builtin` (dialects in `internal/dialects/`),
  `core/platform`, `core/yamlschema`, `dbschema`, `migration/generator`,
  `migration/migrationfile`, `migration/shadow`, `migration/migrator`,
  `migration/planner`, `migration/schemadiff`. Internal: `internal/lexer`,
  `internal/parser`, `internal/dialectlexer`, `internal/convert`,
  `internal/schemaprep`, `internal/modelast`, `internal/tablelookup`,
  `internal/dbschema`, `internal/envbool`, `internal/capabilityprobe`,
  `internal/serverprofile`, `internal/capmatrix`.
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
  fail at parse time.

## The Native CLI Surface

- `ptah` groups verbs into namespaces (`ptah schema render`); `--help` is the
  authority on flags.
- There is no `ptah generate`, `ptah compare`, `ptah read-db`, `ptah drop-all`
  or `ptah migrate`, and none may be added: Atlas spellings live only in
  `ptah-compat`.
- `--dry-run` belongs to the commands that write, not to the whole CLI.
- `[env: PTAH_...]` on a `--help` line marks an environment binding; no marker,
  no binding. Booleans are [strict](#boolean-ptah_-environment-variables-are-strict).
- Rendering is deterministic and dependency-aware; a test may pin its bytes.

## Building And Testing

```bash
make build                       # ptah, ptah-ls, ptah-compat, ptah-integration-test
go test ./... -count=1           # unit contour
make lint                        # golangci-lint, qtlint, nolintguard -- three targets, all required
scripts/check-test-style.sh      # declarative-test baseline
make integration-test            # Docker Compose suite; binds fixed host ports
```

- These are the only test entry points; a wrapper stays thin, with no
  discovery, orchestration or reporting of its own.
- `make docker-clean` prunes the whole Docker daemon; check what is running
  before it or `make integration-test`.
- `make lint` is not the whole lint job (`.github/workflows/go-lint.yml` runs
  policy scripts too). Before finishing, run what the workflows run for the
  files you touched, read from the job.

### Pull-request scoping

- `scripts/ci-diff-scope.sh` alone decides to skip a contour, and fails open.
  Never use a workflow `paths:` filter: a missing check reads as a pass.
- A scoped workflow keeps an always-visible `scope` job and a `*-gate` job
  that fails when decision and results disagree. Every contour runs on a push
  to `master`.
- Gate jobs carry `if: ${{ !cancelled() }}`
  (`scripts/check-ci-scope-gate.sh --check-workflows`): a gate skipped after a
  failed dependency reports success. `always()` is refused too, since it also
  judges a run a newer one canceled.

### The version matrix

- Release lines are declared once, in `internal/capabilityprobe/cells.go`;
  then run `scripts/check-docsync.sh --write`. `go run
  ./internal/cmd/capmatrix matrix` shows the fan-out.
- Probe jobs skip a pull request unless a comment asks: `/capability-matrix`,
  `/capability-matrix postgres`, or prefixes and exact ids. `cells`, `preset
  coverage` and `documented matrix` always run.
- Upstream end of life lowers the promise and removes nothing ("Upstream end
  of life" in `docs/site/src/content/docs/databases/support-policy.md`); a
  line CI stops running records the reason in `Unprobed`.
  `.github/workflows/eol-check.yml` opens a draft pull request when a
  declaration outruns the vendor calendar; `go run ./internal/cmd/eolcheck
  report` does it by hand.
- `Support` describes this repository's testing, not the server, and gates
  nothing. Certified or legacy-tested needs a probe, a server
  `go-integration-tests.yml` starts, or the compiled-in SQLite
  (`TestCells_CertificationMatchesWhatContinuousIntegrationRuns`).
- Only the generated support matrix counts release lines; authored pages never
  repeat its counts (`check-support-matrix.mjs`).
- A provider-served engine declares its lines and levels in the plugin
  (`release-lines.json`); Ptah's matrix links there with no level (ADR 0023).
  The YDB lines in `cells.go` describe the embedded engine and go with it.

### Lint rules

A new lint rule needs its meaning in `internal/lintcatalog`, which holds the
identifier convention and feeds the generated rule page;
`scripts/check-docsync.sh` fails when they disagree. `preConventionCodes` may
shrink, never grow: rename a new rule that fails the convention.

### The Go toolchain

In `go.mod`, `go` is the published compatibility floor and moves on a human
decision; `toolchain` is the CI build and moves every patch release. Never
write a `go-version:` literal into a workflow (use `go-version-file: go.mod`)
or the version into `.golangci.yml`; clear a standard-library advisory by
raising `toolchain`, never `go`. `scripts/check-go-toolchain-single-source.sh`
enforces this; `.github/actions/ptah/action.yml` is its one exemption and
says why.

## Compatibility Policy

Ptah aims to be a drop-in replacement for the Atlas CLI under three rules;
stating only the first is how a capability gets thrown away.

**(a) Never be looser.** What the community binary refuses must not succeed
here. Where Ptah cannot yet implement a construct the community binary enforces,
refuse loudly rather than accept and ignore.

**(b) Matching is the floor, not the ceiling. We do not copy defects.** Where
the community binary silently drops input, corrupts state or fails for an
unrelated reason, do not reproduce it: `-- atlas:txmode none` works without a
blank line after it, and `file()` in `atlas.hcl` stays inside the project,
where the community binary reads `/etc/passwd`. Say so in the commit and
issue: "we are stricter here, deliberately, and here is the measurement."

**(c) Compatibility never removes a capability.** `ptah-compat` is also the
migration path for Atlas Pro pipelines, so:

- the default surface keeps every implemented capability reachable;
- `PTAH_ATLAS_STRICT_COMPAT=1` selects a CE-only policy for reference and
  conformance runs. It refuses extension values; it refuses Atlas txtar, Ptah
  directives and SQL templates where a command executes, converts or replays a
  migration body, but keeps the bytes in a checksum-only read (`migrate hash`,
  `migrate status`, `migrate validate` without `--dev-url`) for `atlas.sum`
  parity; it refuses the known `PTAH_*` toggles, never a `PTAH_*` prefix,
  since `atlas.hcl` reads user input through `getenv`;
- the strict selector is an environment variable, never a flag (the
  `cli-surface` conformance tier asserts flag parity);
- what the default leaves out is reported, not dropped in silence;
- the capability is written down: a feature-matrix row, documentation, a test.

To decide whether to match, ask what it costs the user. A different spelling
of the same outcome (wording, exit codes, flag names, output shape): match. A
loss of something they asked for: do not.

### Boolean `PTAH_*` environment variables are strict

- Absence selects the default; a present value that is not a boolean makes
  the owning command refuse before work. **Never turn a parse error into the
  default.** Use `os.LookupEnv`: `PTAH_X=` is not absence.
- Declare each variable once with `envbool.New(name, default, class)` and
  read it with `Var.Resolve` (`internal/cli/internal/envboolguard` refuses
  `strconv.ParseBool(os.Getenv(...))`). `class` is `Gated` (CE lacks it;
  strict mode refuses it), `Retained` (CE has it; strict mode keeps it) or
  `Selector` (reserved), with a comment on what the pinned binary has. An
  unclassified declaration fails closed.
- **Resolve the variables a command owns before its early returns**, on every
  invocation of each command that recognizes them and no other, so a malformed
  value cannot stay dormant.
- Toggles opt in to the permissive side, so a typo fails closed; a new
  restrictive boolean documents why it cannot be a capability gate.

### A `PTAH_*` value is consumed once, by the surface that decides with it

`internal/cli/internal/cmdadapter` installs the same `PTAH_*` binding on the
native target, which can then read a variable its adapter scoped away. **When
an adapter resolves a native flag's whole value, disable the target's binding
with `cmdflags.DisableEnvBinding`**, and test the refusal plus a control in
which the variable still reaches an unscoped run.

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

### Compatibility with older Ptah is not owed

Ptah is pre-GA: until v1, prefer the cleaner architecture and keep no
fallback, alias, wrapper, old spelling or second reader for an older shape or
an earlier Ptah's output; update callers, tests and docs. Never soften a
refusal or keep a default to preserve output. Say so in the issue and commit:
"this changes behavior; pre-v1, so no compatibility is owed." This never
justifies breaking parity with the community binary or discarding user data.

## Native And Compatibility Capability Ownership

`ptah-compat` is an adapter over Ptah capabilities, not a second product; the
`ptah` binary stays purely native.

- A **general capability** means something without Atlas (schema plan
  testing, migration testing, drift detection, schema security analysis,
  checkpoints, pre-apply checks, planning, validation, artifact publishing,
  directory import). It lives in a shared package below the CLI, reachable
  natively, never only in `internal/cli/atlas`.
- **Compatibility machinery** may stay compat-only: `atlas://` resolution,
  flag spelling and precedence, `atlas.hcl` evaluation, the `.plan.hcl` and
  `.test.hcl` codecs, revision-table and checksum format, Atlas-shaped output,
  exit codes, diagnostics. A codec feeding a shared capability is the
  intended shape.
- Adapt the compat surface to native capabilities; never narrow a native one
  to what Atlas can express. A deliberate divergence is a policy the caller
  picks in the shared package, not a second implementation.
- A capability reaches the native surface as a verb or flag; the compat
  surface takes no new flag and keeps fuller behavior behind a `PTAH_*`
  variable, as `PTAH_ALLOW_EXTERNAL_SCHEMA` does.
- **Every PR that adds behavior under the compat surface says which it is**:
  `GENERAL CAPABILITY` (where it lives, how native Ptah uses it, and if no
  native surface is added, why and which issue tracks it) or `COMPATIBILITY
  ADAPTER` (naming the Atlas contract). This applies to new work.

### Dependency direction

Native code and shared packages never import `internal/cli/atlas`. **No gate
checks this**: `scripts/check-architecture-boundaries.sh` and
[`docs/architecture_boundaries.md`](docs/architecture_boundaries.md) do not
name it. Besides `cmd/ptah-compat`, only `internal/cmdrefviews` imports it, to
show what strict mode removes, and says why at the top of the file; a second
importer needs a written reason too.

## Language And Spelling

- American English in code, comments, docs, issues, PRs and CLI output,
  except exact external quotes and protocol tokens.
- **Plain international English in everything you write**, including commits,
  PR and issue text, comments, CLI output and review replies: short
  sentences, concrete nouns, active voice, one idea per sentence, for readers
  who are often not native speakers. Say what changed and why, once.
  [`docs/STYLE_GUIDE.md`](docs/STYLE_GUIDE.md) is the authority (sections 4
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

## Documentation Obligations

**Before finishing a change to external behavior, inspect and update the
documentation.** External behavior: CLI names, flags, environment variables,
help, output formats, exit codes; config formats, keys, validation,
precedence; generated and parsed SQL, migration files, directives, revision
tables, hash files; public Go APIs and extension points; `ptah-compat`
behavior, conformance claims, documented limitations; user-facing errors,
warnings, diagnostics and safety checks.

- Search old and new terms (commands, flags, variables, issue numbers, exact
  errors) in `README.md`, `docs/`, `docs/site/src/content/docs/`,
  `examples/**/README.md`, `integration/*.md`, package READMEs, and this file
  when rules change.
- Never document Atlas command paths in the native binary, and never claim
  full Atlas parity without conformance evidence.
- **Run the gates `.github/workflows/docs.yml` runs**: `node
  docs/site/scripts/check-style.mjs` (no install, every Markdown file), `npm
  run check:*` from `docs/site` after `npm ci`, and `npm run build`. The style
  guide holds the page rules (section 7 terms, 8 code and quick starts, 11
  visuals, 15 checklist, 17 translations).
- Terms live in `docs/site/scripts/data/terminology.json` (read by
  `check-terminology.mjs` and `internal/cli/internal/terminologyguard`); render
  section 7 with `--write`. Product definitions live once in `src/glossary.ts`.
- A new page joins `docs/site/src/sidebar.mjs` and the route ledger (`node
  docs/site/scripts/check-route-retirement.mjs --write`); a retired route gets
  a `redirectRoutes` entry. A route added and renamed on an unmerged branch
  leaves the ledger only by `--forget <route> --against origin/master`, never
  by a redirect for a URL nobody was served. `docs/site/scripts/lib/docroutes.mjs`
  says which routes exist; a route is Astro's, not the file path's.
- The content inventory is computed by `buildContentInventory()` in
  `docs/site/scripts/build-content-inventory.mjs` (`--print`), never committed.
- `docs/site/src/content/docs/atlas/feature-matrix.md` is generated: edit
  `docs/site/scripts/data/feature-matrix-rows.json`, run `node
  docs/site/scripts/build-feature-matrix.mjs`. A `note` is the reader's text
  and never restates a count or list that code or a generated table owns;
  `evidence` is not rendered. When a verdict flips, re-tense the old reasoning
  as history and keep its measurement (`check-matrix-verdict-prose.mjs`).
- A page claims features with `owns:`; `scripts/check-feature-inventory.sh
  --write` regenerates `docs/feature-inventory.json`. Keep the design
  `internal/featureinventory` documents: exact strings, a claimed-row floor in
  source, columns named for what is checked (never "canonical"), program rows
  from `.goreleaser.yaml`, not `go list`.
- A quick start sets `quickstart: true`; CI runs its commands on three
  platforms (`internal/quickstart`, `scripts/check-quickstart.sh`), and
  `quickstartContinues:` chains pages in one working directory. The inference
  quick start has its own Compose fixture; on a remote Docker context run
  `PTAH_DOCKER_CONTEXT=<context> PTAH_FIXTURE_HOST=<host> docs/site/scripts/check-inference-quick-start.sh`.
- `check-translations.mjs` checks only the structure and bytes of
  `README.ja.md`, `README.de.md` and `README.fr.md`; review their meaning
  yourself, accessibility text included.
- `support-matrix.md` is generated, `support-policy.md` the promise,
  `support-evidence.md` the measurement; generated counts stay off the last
  two.
- After a visual fixture or generator changes, run `PTAH_BIN=../../bin/ptah
  npm run assets:write` in `docs/site` and look at the PNGs in
  `docs/site/src/assets/`; `check-generated-assets.mjs` compares only
  `docs/site/public/samples`.
- Each top-level `examples/*` has the sections `check-examples.mjs` requires;
  `npm run examples:write` rebuilds the index, and `npm run check:examples`
  and `scripts/check-examples.sh` verify it.
- Deep documentation work: `.agents/skills/ptah-documentation-maintenance/SKILL.md`.

### Label every issue you file

Only web-form issues get a type label, and an unlabeled issue escapes every
planning filter, so pass labels to `gh issue create`: one type (`bug`,
`enhancement`, `feature-request`, `documentation`, `question`); areas only
where clear (`cli`, `migration`, `schema-generation`, `postgresql`, `sql`,
`rls`, `roles`, `security`, `foreign-key`, `constraints`, `field-ordering`);
`post-ga` or `critical` when they apply. Engine names have no label; classify
by subject. Milestones are not used. Prefer no label to a wrong one.

`frozen` stops work for the reason its comments give; it is not ownership,
and a stale reason is raised on the issue, not decided alone.
`hold-off-merge` means what it says. `eol-keep` on a CLOSED end-of-life pull
request marks its declaration deliberate, so the daily check opens no new
pull request for that line.

## Code Style And Linting

- `.golangci.yml` is a strict contract: fix code, never relax thresholds or
  widen exclusions. Run `golangci-lint run --fix ./...`, then `golangci-lint
  run ./...`, fix the rest by hand, and repeat both with
  `--build-tags="$(scripts/list-build-tags.sh --joined)"`, since CI lints both
  contours (`make lint-golangci`). Every discovered tag except `integration`
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

## Testing Standards

### Where tests live

- A test crossing no process, filesystem, network or database boundary is a
  unit or pipeline test beside its package.
- Integration tests live under an `integration/` tree with `//go:build
  integration` and are black-box (`package <name>_test`, exported API, never
  `*_internal_test.go`). Those trees hold only tagged tests; their library
  code lives outside, with its own unit tests.
- For behavior reachable only through an unexported symbol, move the logic
  behind a package boundary with a black-box unit test, or add the real public
  boundary. Never export a test-only accessor or use white-box access as an
  integration shortcut.
- `internal/testcontour` refuses tagged tests elsewhere, and CI's `go run
  ./internal/cmd/testcontour --tags integration` fails on any skip. Never
  select integration tests by build tag, package or name pattern.
- **A live test asks for an engine, never for a variable**: `dbtarget.URL(c,
  dbtarget.PostgreSQL)` for the address ptah connects with,
  `dbtarget.DriverDSN(c, dbtarget.MySQL)` for a raw driver. They differ:
  `go-sql-driver/mysql` reads a `mysql://` prefix as part of the username.
  Never `os.Getenv` a database address in a test
  (`TestNoIntegrationTestReadsARegistryVariableDirectly`).
- File names say what a test needs: `*_live_test.go` a server,
  `*_e2e_test.go` a command or process (the larger claim, when both),
  `*_fs_test.go` only the filesystem; no new `*_integration_test.go`. Classify
  by reading, not by scanning imports: the database helper often sits in a
  sibling file.
- `cmd/integration-test` covers apply, rollback, idempotency, parallel
  execution, partial-failure recovery and schema diff.

### An engine-observable change carries an engine-level test

A unit test shows the statement Ptah built, not that a server accepts it:
rows planned in an order a foreign key rejects pass every offline assertion.
A change needs a test under `integration/` when the diff:

- changes the SQL an engine receives, or its order;
- depends on a constraint, permission, transaction or catalog rather than on
  Ptah's code;
- gives a CLI verb a database effect, or moves one to another verb, phase or
  connection.

Use `*_live_test.go` where a server decides and `*_e2e_test.go` where the
binary does, `dbtarget` for the engine, rows varied by engine where answers
differ, and expected values read back from the database. **The unit test
stays**: it names the defect; the engine test proves it matters. Nothing is
owed where Ptah settles the behavior itself (a pre-connect refusal, a
rendering, a parse, a rank over a fixture) or the engine normalizes it away;
a change that meets no condition says in the pull request which it misses.
An engine a new test needs joins `.github/workflows/go-integration-tests.yml`
in the same change.

**A provider-served engine is tested in the provider's repository.** Ptah's
CI proves the provider contract only on `provider-sdk/testdata/fixture`
(`testutils.BuildFixtureProvider`, `provider-sdk/integration`,
`integration/providercli`), and a change to what a provider receives is
tested there. Once `stokaro/ptah-ydb-plugin` serves `ydb://`, YDB end-to-end,
live and parity tests run in that repository against current Ptah and never
join `go-integration-tests.yml`; until then embedded YDB follows the rules
above (ADR 0031).

### Declarative tests only

No `if`, `switch` or `goto` in a test function; `for` over a static table is
fine; `Example*` is exempt. New tests add nothing to the
`scripts/check-test-style.sh` baseline, refreshed only through the script's
`--write-baseline`, never the bare tool. Never use `testify` (`depguard` refuses
it); `importas` requires `quicktest` as `qt`.

### Checkers belong to the test they report against

`QTLINT_RULES` in the Makefile enforces these in both contours and on Windows
(`ptah-go-lint-windows` runs `make lint-qtlint` and golangci-lint for
`_windows_test.go`, apart from `windows-unit-tests` so neither hides the
other). `make lint-qtlint-fix` rewrites, one rule per pass.

- Assert through a receiver: `c := qt.New(t)`, then `c.Assert(...)`.
- Subtests use `t.Run(name, func(t *testing.T) { c := qt.New(t) ... })`,
  never `c.Run`, whose closure asserts through the parent. The parameter `t`
  shadows the outer one on purpose.
- A helper takes the checker, `c *qt.C`, not `testing.TB`, which hides the
  escape from `-require-testing-run`. One that needs `*testing.T` takes it,
  never `c.TB.(*testing.T)`.

### A table row carries data, not a checker

- Rows hold the varying value (`wantErr: "..."`), not an asserting closure;
  rows whose assertions differ are two tests.
- Callbacks into production code are fine; assertion callbacks, checker
  factories and assertion-choosing comparators are not. `-require-data-rows`
  catches only a function field taking `*qt.C`; this rule covers the rest.
- No helper hides the conditional (`checkError(c, err, wantIs, wantLike)`
  choosing a checker); assert per case.
- **A rewritten table that still passes proves nothing**: disable the defect,
  watch it fail, restore. Losing a row's discrimination is worse than the old
  shape.

### Separate happy-path and failure-path tests

`TestXxx_HappyPath` asserts `err, qt.IsNil` and the value;
`TestXxx_FailurePath` asserts the error and the zero value; never one table
with a `wantErr` branch. Prefer `qt.ErrorIs` for a sentinel, `qt.ErrorAs` for
a type, `qt.ErrorMatches` when there is no sentinel.

```go
func TestDialectFromURL_FailurePath(t *testing.T) {
	t.Run("unsupported", func(t *testing.T) {
		c := qt.New(t)
		got, err := atlasurl.DialectFromURL("spanner://localhost/dev")
		c.Assert(err, qt.ErrorMatches, `unsupported --dev-url dialect "spanner://localhost/dev"`)
		c.Assert(got, qt.Equals, "")
	})
}
```

### Black-box by default, white-box as the exception

Tests are `package <name>_test` over exported API. White-box is allowed only
for an unexported function critical to correctness or otherwise unobservable
state, in `*_internal_test.go` whose first line after `package` is
`// White-box testing required:` with the reason, never in an integration
tree.

### A linter's reach is configuration, not a property of the linter

`rowserrcheck` sees only packages its settings list as declaring `Query`,
which must include Ptah's `sqlrunner.Runner` readers (see `.golangci.yml`);
`internal/rowserrguard` checks the list against the tree both ways. It does
not follow rows into closures or parameters: a function running its own
`QueryContext` owns the final check. A sweep hit is not a finding:
`Rows.Err` is nil until `Rows.Next` runs, so verify each site. Run a new
linter against the pre-fix tree before trusting a clean result.

### A blank import says why

Each blank import has its own comment on or right above its line; a group
comment does not count, since `gofmt` interleaves groups. revive's
`blank-imports` covers code and `internal/blankimportguard` covers tests,
where a missing registry import quietly shrinks what a test checks.

### A rule with no caller is a rule that is not in effect

`unused` counts tests as callers, so a rule called only by its own tests is
green while the behavior is absent (`internal/embedguard` reports this over
`internal/embed...`). A new
rule needs a test that drives the real entry point and fails with the rule
disabled. A guard finding is a decision: call the rule or delete it.
