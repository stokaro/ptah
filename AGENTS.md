# AGENTS.md

This file gives coding agents repository-local guidance for working in Ptah.

It holds the rules no gate can enforce: decisions that have to be right before
a check could run, and habits whose failure a green build does not show. Where
a gate exists, this file names it instead of restating what it checks, because
a restated list drifts the moment the gate moves.

## Where Work Happens

**Every change is made in a linked git worktree. Never work in the main
worktree.** Two agents work here at once, routinely; a branch checked out in
the shared main worktree moves the other agent's work to a branch it did not
ask for.

```bash
git fetch origin
git worktree add .claude/worktrees/<name> -b <branch> origin/master
git worktree add .codex/worktrees/<name>  -b <branch> origin/master
```

`<name>` describes the task -- `2538-constraint-include`, not `tmp`. Branch
from `origin/master`, never from local `master`, which is only as current as
the last pull and makes every "measured against master" statement wrong.

**Clean up when the work merges.** `git worktree remove <path>`, then delete
the branch with `-D` -- a squash-merged branch is not "merged" to `git branch
-d`, so confirm the content reached master first. **Never remove a worktree you
did not create**: an existing path is another agent's claim on that task, and
`--force` on it destroys work that has no pull request yet.

**A gate that enumerates files asks `git ls-files`, never the filesystem.** A
linked worktree's root is an ordinary directory, so a walk descends into every
checkout parked under the repository and reports another branch's files as this
one's. `scripts/check-test-style.sh` and `scripts/list-go-modules.sh` are the
shape. Two more properties make a check a gate rather than a ceremony: one
that regenerates requires every generator to exit 0, since a generator that
crashes before writing leaves the tree as clean as one that confirmed every
file; and one that enumerates holds its corpus above a floor, since a glob
that stopped matching reports zero findings and reads as success.

## What Ptah Is

Ptah generates SQL DDL from annotated Go structs, compares a desired schema
against a live database, and plans, generates and applies migrations. It ships
two binaries: the native `ptah`, and `ptah-compat`, a drop-in replacement for
the Atlas CLI.

Read facts out of their declaration rather than out of a count written here:

- Accepted dialects: `core/platform.NormalizeDialect` and
  `core/platform/constants.go`. `migrations lint` and `sql lint` leave Oracle
  out deliberately; `internal/lintdialect/dialect.go` says why.
- Which driver a URL takes: `databaseDriverConfig` in `dbschema/connection.go`.
  It is not one driver per dialect, and a `sqlite` URL may take `libsql`.
- What a dialect renders: `docs/capabilities.md`. Acceptance is not a promise;
  `SERIAL` is refused by name on ClickHouse, CockroachDB and Spanner rather than
  downgraded silently.

## Repository Layout

Most implementation sits under `internal/` and cannot be imported from another
module, so check where a package lives before writing an import path.

The public Go surface is the ledger [`docs/public_api.md`](docs/public_api.md),
enforced by `scripts/check-public-api.sh` (every importable library package is
classified, under Stable Embedder API or Documentation-Only Packages),
`scripts/check-public-api-released.sh` (`apidiff` against the newest tag; an
intentional break needs a reviewed entry in `docs/public_api_approvals.txt`) and
`scripts/check-exported-docs.sh` (every exported declaration documented). Only
the first gate reads both categories; the rest read the stable one, because a
sample package documentation reaches carries no guarantee.

"Library package" is package metadata, not a path: a `main` package and a
directory holding only `_test.go` files publish no import path and are outside
the surface for that reason. What is left is behind an `internal/` boundary --
matched as a whole path segment, so `core/internal` is a boundary and
`internalized` is not. The gate still carries a named exemption per subtree
whose move has not landed (`cmd`, `integration`, `stubs`); each is deleted by
the change that internalizes its subtree under stokaro/ptah#2974, and none may
be added. Additive API changes get normal code review: do not commit a generated
snapshot of exported declarations to make them show up twice in a diff
(`docs/public_api.snapshot` and its gate were removed in stokaro/ptah#2572 for
that reason).

Core packages a reader meets first: `core/ast`, `core/astbuilder`,
`core/goschema` (annotation parser in `parser.go`), `core/renderer` (dialects
under `internal/dialects/`), `core/platform`, `core/yamlschema`, `dbschema`,
`migration/generator`, `migration/migrationfile`, `migration/shadow`,
`migration/migrator`, `migration/planner`, `migration/schemadiff`.

Internal packages worth knowing: `internal/lexer`, `internal/parser` and
`internal/dialectlexer` (SQL tokenizer and DDL parser); `internal/convert/...`
(remaining representation conversions; stokaro/ptah#2725 owns removing the SQL
one); `internal/schemaprep` (model-to-model preparation shared by renderers and
planners); `internal/modelast` (lowering to AST nodes); `internal/tablelookup`;
`internal/dbschema/...` (per-dialect readers and writers); `internal/envbool`
(the one grammar for boolean `PTAH_*` variables); `internal/capabilityprobe`,
`internal/serverprofile` and `internal/capmatrix` (declared release lines, what
a live server established, and the CI fan-out over them).

Command tree: `cmd/ptah/main.go` is the native binary and `cmd/root/root.go`
assembles it from the namespaces `cmd/schema`, `cmd/db`, `cmd/migrations`,
`cmd/oci`, `cmd/seed`, `cmd/sql`, `cmd/viz`, `cmd/introspect`, `cmd/version`
and `cmd/license`; each leaf verb keeps its own package below them
(`cmd/generate` backs `ptah schema render`). `cmd/atlas` is the Atlas-compatible
tree shipped by `cmd/ptah-compat`. `cmd/integration-test` is the suite runner
and `cmd/ptah-ls` the language server. Both command trees are adapters; see
[Native And Compatibility Capability Ownership](#native-and-compatibility-capability-ownership).

Entities to test against: `stubs/`, `examples/`, and the numbered fixture
series in `integration/fixtures/entities/`.

## Schema Annotations

Directives are `//ptah:` comments on Go structs; `stubs/` holds worked
examples. Two things the parser will not tell you:

- **An index annotation has to sit on a struct field.** Written at file level,
  after the closing brace, it contributes no index and says nothing. To declare
  an index away from its column, give it a holder struct and name the table:
  `//ptah:schema:index name="..." fields="name" table="products"` on a `_ int`
  field.
- `fields=` is the modern spelling of the column list; `columns=` is a legacy
  synonym. Unknown attribute names are rejected at parse time.

## The Native CLI Surface

`ptah` groups verbs into namespaces (`ptah schema render`, `ptah db read`,
`ptah migrations up`); `--help` is the authority on flags. There is no `ptah
generate`, `ptah compare`, `ptah read-db`, `ptah drop-all` or `ptah migrate`,
and none may be added: Atlas spellings live only in `ptah-compat`.

`--dry-run` belongs to the commands that write, not to the CLI as a whole. A
flag's `--help` line prints `[env: PTAH_...]` when it reads an environment
variable; one without the marker has no binding. Boolean variables are strict
-- see [below](#boolean-ptah_-environment-variables-are-strict).

Schema rendering is deterministic and dependency-aware: two runs over the same
entities produce byte-identical output, and a test may pin that.

## Building And Testing

```bash
make build                       # ptah, ptah-ls, ptah-compat, ptah-integration-test
go test ./... -count=1           # unit contour
make lint                        # golangci-lint, qtlint, nolintguard -- three targets, all required
scripts/check-test-style.sh      # declarative-test baseline
make integration-test            # Docker Compose suite; binds fixed host ports
```

`make docker-clean` runs `docker system prune -f` against the whole daemon,
not only the suite's resources, so look at what is already running before
invoking it or `make integration-test`. These are the only test entry points:
a convenience script may wrap them thinly but must not become a second
discovery, orchestration and reporting implementation (stokaro/ptah#2507).

`make lint` is not the whole lint job: `.github/workflows/go-lint.yml` runs
policy scripts beside it. Before finishing, run what the workflows run for the
files you touched -- read the job, do not trust a list in prose.

### Pull-request scoping

Pull-request workflows classify the whole diff before skipping an expensive
contour. `scripts/ci-diff-scope.sh` is the one authority and fails open; a
workflow-level `paths:` filter is not a substitute, because a filtered workflow
leaves no check on the pull request at all, and an absent check reads exactly
like a passing one. Each scoped workflow keeps an always-visible `scope` job and
a `*-gate` job that fails when the decision and the job conclusions disagree.
Every gate job carries `if: always()`, and
`scripts/check-ci-scope-gate.sh --check-workflows` refuses one that does not: a
gate skipped because a dependency failed reports success about the failure it
exists to surface. Every contour runs on a push to `master`.

### The version matrix

Release lines are declared once, in `internal/capabilityprobe/cells.go`;
adding one is a literal there plus `scripts/check-docsync.sh --write`.
`go run ./internal/cmd/capmatrix matrix` says what the pipeline fans out over.

The probe fan-out does not run on a pull request by default
(stokaro/ptah#2185). Request it from the pull request with a comment:
`/capability-matrix`, `/capability-matrix postgres`, or a mix of prefixes and
exact ids. The cheap jobs -- `cells`, `preset coverage`, `documented matrix` --
run on every pull request regardless.

A cell's `Support` level describes **this repository's testing**, not the
server, and nothing reads it to gate an operation. A line may claim certified
or legacy-tested only if something actually runs against it -- the probe, a
server `go-integration-tests.yml` starts, or for SQLite the compiled-in engine.
`TestCells_CertificationMatchesWhatContinuousIntegrationRuns` measures that
rather than trusting the literal. The generated support matrix is the only
release-line census; authored pages must not repeat its counts
(`check-support-matrix.mjs`).

### Lint rules

Every rule identifier is enumerated on one generated page. Adding a rule is
two edits -- the rule, and its meaning in `internal/lintcatalog` -- and
`scripts/check-docsync.sh` fails when they disagree. The identifier convention
lives in `internal/lintcatalog` too, with the identifiers that predate it
pinned in `preConventionCodes`; that list may shrink and must not grow, so a
new rule that fails the convention check is renamed, never appended.

### The Go toolchain

`go.mod` carries two Go versions with different lifecycles. `go` is the
published compatibility floor and moves on a human decision; `toolchain` is
what CI builds with and moves on every patch release. Never write a
`go-version:` literal into a workflow (every `setup-go` reads
`go-version-file: go.mod`), never restate the version in `.golangci.yml`, and
never raise `go` to clear a standard-library advisory -- raise `toolchain`.
`scripts/check-go-toolchain-single-source.sh` enforces the single source; the
one exemption, `.github/actions/ptah/action.yml`, forwards its caller's inputs
and says why.

## Compatibility Policy

Ptah aims to be a drop-in replacement for the Atlas CLI. The goal has two
halves, and stating only the first is how a capability gets thrown away.

**Never be looser.** An invocation the community binary refuses must not
succeed here; accepting it means a user's mistake passes silently and fails
somewhere later. Where Ptah cannot yet implement a construct the community
binary enforces, refuse loudly rather than accept and ignore.

**Matching is the floor, not the ceiling. We do not copy defects.** Where the
community binary's behavior is a defect -- it silently drops something the
author wrote, corrupts state, or fails for a reason unrelated to the request --
reproducing it is a wrong answer. Two measured examples: `-- atlas:txmode
none` is honored with or without a blank line after it, where the community
binary silently drops the directive without one; `file()` in `atlas.hcl` is
confined to the project directory on both binaries, where the community binary
reads `/etc/passwd` (stokaro/ptah#1042). When the halves pull apart, say so in
the commit and the issue: "we are stricter here, deliberately, and here is the
measurement" is a complete answer.

**Compatibility never removes a capability.** `ptah-compat` is the migration
path for Atlas Pro pipelines too, so reaching CE compatibility must never mean
deleting a capability Ptah models. The shape that satisfies both halves:

- the default compatibility surface keeps every implemented capability
  reachable;
- `PTAH_ATLAS_STRICT_COMPAT=1` selects a separate CE-only policy for reference
  and conformance runs. It refuses extension values, and refuses Atlas txtar,
  Ptah directives and SQL templates where a command executes, converts or
  replays a migration body -- a checksum-only read (`migrate hash`, `migrate
  status`, `migrate validate` without `--dev-url`) preserves the bytes, because
  CE hashes them and `atlas.sum` parity depends on it. It refuses the known
  `PTAH_*` feature toggles -- but never an arbitrary `PTAH_*` name by prefix, since
  `atlas.hcl` reads ordinary user inputs through `getenv`;
- the strict selector is an environment variable, never a flag: the
  conformance `cli-surface` tier asserts flag parity with the pinned binary;
- what the default leaves out is reported, not dropped in silence;
- the capability is written down -- feature-matrix row, documentation, a test.

**Deciding which you are doing.** Before matching a measured behavior, ask what
it costs the user. "Nothing, it is a different spelling of the same outcome"
-- match it; wording, exit codes, flag names and output shape are worth being
identical on. "They lose something they asked for" -- do not.

### Boolean `PTAH_*` environment variables are strict

Absence selects the default; a present value must parse as a boolean or the
owning command refuses before doing work. **Never convert a parse error into
the default**, and use `os.LookupEnv`, because `PTAH_X=` and an absent
variable are different configuration states.

Declare each variable once with `envbool.New(name, default, class)` and
resolve it through `Var.Resolve`; `cmd/internal/envboolguard` refuses a
`strconv.ParseBool(os.Getenv(...))` call site. `class` is the strict-mode
classification, stated at the declaration with a comment saying which
capability the pinned binary has or lacks: `Gated` (adds behavior CE does not
have; strict mode refuses it), `Retained` (restores something CE does; strict
mode keeps it), `Selector` (reserved). An unclassified declaration fails closed.

**Resolve the variables a command owns before its early returns.** A malformed
value must not stay dormant because this invocation did not reach the branch
that reads it. Validate on every invocation of the command that recognizes the
variable, and on no others. Toggles opt in to the more permissive side, so a
typo fails closed; do not add another restrictive boolean without documenting
why it cannot be a capability gate.

### A `PTAH_*` value is consumed once, by the surface that decides with it

The compatibility surface forwards to a native command, and
`cmd/internal/cmdadapter` installs the same `PTAH_*` binding on the target. So
a variable is offered twice, and when the adapter's decision was *nothing* --
a scope that emptied the forwarded values -- the target reads the variable
itself and the decision is overwritten by its own input (stokaro/ptah#1535).
**When an adapter resolves a native flag's whole value, disable the binding on
the target with `cmdflags.DisableEnvBinding`.**

Two tests, not one: the refusal proves the scope closes, and a control proving
the variable still reaches an unscoped run proves the closure was not achieved
by dropping the variable outright.

### Recognition that spans two functions belongs to one of them

When two functions in a pipeline must recognize the same set, give them one
predicate and say at its declaration why it cannot become two. Two lists agree
when the second is written and stop agreeing when the first is extended, and
testing either end alone cannot see it (stokaro/ptah#1540). The control that
catches it drives the public path that joins them.

### A path is not a string, and an assertion about one is not portable

Most tests that fail on `windows-latest` are not about Windows. The habits,
each of which reads as correct on Linux:

1. A path interpolated into HCL, YAML or a Go template: `\U` is an escape.
   Use `filepath.ToSlash` or `strconv.Quote`.
2. A path in `url.URL.Path` rendered with `String()`: `%5C`, and `//` before
   a drive letter. Use `atlasurl.SQLiteURLFromPath`.
3. An assertion on text the OS wrote: assert with `errors.Is`, or match only
   the Ptah-authored part. `testutils.StatMissingText` and
   `syscall.ENOENT.Error()` derive the OS clause where it must appear.
4. A program that has to exist: `go build -o dir/tool` is not executable on
   Windows; `internal/exeext` carries the extension for both ends.
5. `os.PathError.Op` is `stat` on Unix and `GetFileAttributesEx` on Windows.
6. A shell's grammar in a fixture: `internal/preflight` is portable, `echo m;
   exit 9` is not. `testutils.FailingHookCommand` renders each spelling.
7. Win32 refuses `< > : " / \ | ? *` in a file name. Split the test by
   concern, not by platform: the filesystem round trip uses a name every
   platform allows, and the escaping of the reserved character is asserted as
   a string on every platform.
8. `os/exec` does not set `PWD` on Windows; Ptah's contract does, so Ptah sets
   it.

Two rules follow. **A platform-conditional assertion tends to pass on the
platform it cannot test** -- a file-mode check reduces to `0o200 == 0o200`
there -- so take the varying part as a parameter or split by build tag; never
write one assertion that becomes a tautology on the half you are not looking
at. **A wildcard is not a wildcard across lines**: `qt.ErrorMatches` anchors
the whole message and `.` does not match a newline, and Windows writes two
lines often enough to matter; assert the sentinel with `qt.ErrorIs` and pin a
prefix with `(?s)`.

A rule about what a file may name must not depend on the machine reading it:
`filepath.IsAbs` answers false on Windows for `/tmp/x`, so the `atlas.hcl`
confinement refuses a leading slash, a leading backslash and a drive letter on
every platform.

### A handle is released by a caller, not by the collector

A type that acquires an operating-system handle owes its caller a way to give
it back. On Windows an open directory handle blocks removal, so "the finalizer
closes it" describes a directory nothing can clean up until the collector
happens to run. Add `Close`; make releasing twice and releasing after the handle
is gone both no-ops, so `defer` beside the constructor is always correct; look
for the abandonment path in production, not only in tests; and say which
release happened. The check that a release released is a white-box assertion
on the handle field, and the file says why the black-box version cannot exist.

### Compatibility with older Ptah is not owed

Until v1, compatibility with Ptah's own previous behavior is not a goal. Keep
no fallback, alias, tolerated old spelling or second reader because an earlier
Ptah produced it; do not soften a refusal or carry a default because changing
it would alter existing output. Say so plainly in the issue and the commit --
"this changes behavior; pre-v1, so no compatibility is owed." This does not
license breaking parity with the community binary or discarding user data.

## Native And Compatibility Capability Ownership

`ptah-compat` is an adapter over Ptah capabilities, not a second product.

A **general capability** means something without Atlas -- schema plan testing,
migration testing, drift detection, schema security analysis, checkpoints,
pre-apply checks, planning, validation, artifact publishing, directory import.
Its semantics live in a shared package below the CLI layer and are reachable
through the native surface too; never implement general behavior only inside
`cmd/atlas`.

**Compatibility machinery** exists to interpret or reproduce an Atlas contract
-- `atlas://` resolution, flag spelling and precedence, `atlas.hcl` evaluation,
the `.plan.hcl` and `.test.hcl` codecs, revision-table and checksum
representation, Atlas-shaped output, exit codes and diagnostics -- and may stay
compat-only. A codec feeding a shared capability is the intended shape.

Where native Ptah already has the capability, adapt the compatibility surface
to it; never narrow the native capability to what the Atlas contract can
express. Where the two surfaces deliberately diverge, the divergence is a
policy the caller selects in the shared package, not a second implementation.
Exposing a capability natively means a native verb or flag; the compatibility
surface takes no new flag and keeps fuller behavior behind a `PTAH_*` variable
(precedent: `PTAH_ALLOW_EXTERNAL_SCHEMA`).

Native code must not depend on `cmd/atlas`, and shared packages must not
either. **No gate checks this direction**: `scripts/check-architecture-boundaries.sh`
enforces only the four ADR 0001 directions, none of which names `cmd/atlas`,
so a new non-test importer is caught by review alone. One exists beside the
binary `cmd/ptah-compat`: `internal/cmdrefviews`, which has to construct both
trees in one process to say what strict mode takes out, and says so at the top
of the file. A second needs the same kind of reason written beside it.

**Every PR that adds behavior under the compatibility surface says which it
is**: `GENERAL CAPABILITY` (where the implementation lives, how native Ptah
consumes it, and if no native surface is added, why and which issue records
the gap) or `COMPATIBILITY ADAPTER` (naming the Atlas contract). The rule is
prospective; the repository-wide audit is stokaro/ptah#1213.

## Language And Spelling

American English in code, comments, documentation, issue and PR text, and CLI
output, unless preserving an exact external quote or protocol token.

## Documentation Obligations

**Before finishing any change that affects external behavior, inspect and
update the documentation.** It is a required verification step, not an
opportunistic cleanup. External behavior includes:

- CLI names, flags, environment variables, help, output formats, exit codes;
- config formats, accepted keys, validation and precedence;
- generated and parsed SQL, migration files and directives, revision tables,
  hash files;
- public Go APIs and documented extension points;
- `ptah-compat` behavior, conformance claims, documented limitations;
- user-facing errors, warnings, diagnostics and safety checks.

Search for both old and new terms -- command names, flags, variables, issue
numbers, exact error strings -- across `README.md`, `docs/`, the site content
under `docs/site/src/content/docs/`, `examples/**/README.md`,
`integration/*.md`, package READMEs, and this file when the rules change.
Never document Atlas command paths inside the native binary, and never claim
full Atlas parity unless the conformance evidence proves it.

Follow [`docs/STYLE_GUIDE.md`](docs/STYLE_GUIDE.md). Section 7 is the
terminology authority; its table is generated from
`docs/site/scripts/data/terminology.json`, which is what
`check-terminology.mjs` and `cmd/internal/terminologyguard` read, so add a term
there and render it with `--write`. Product definitions live once in
`src/glossary.ts`.

**Run the gates the workflows run.** `.github/workflows/docs.yml` is the
authority; a subset is `node docs/site/scripts/check-style.mjs`, which needs no
install and governs every Markdown file in the repository, and the `npm run
check:*` scripts from `docs/site` after `npm ci`. `npm run build` is a gate
too: a page every checker accepts can still fail to render.

What a page change needs, beyond passing the gates:

- **A new page** joins `docs/site/src/sidebar.mjs` and the route ledger
  (`node docs/site/scripts/check-route-retirement.mjs --write`) in the same
  change; a retired route gets a `redirectRoutes` entry. A route this branch
  added and renamed before it merged is dropped from the ledger with
  `node docs/site/scripts/check-route-retirement.mjs --forget <route> --against origin/master`,
  never with a redirect for a URL nobody was served and never by editing the
  ledger by hand -- the gate's own finding says to add a redirect, and no gate
  refuses one. A route is Astro's,
  not the file path's -- `docs/site/scripts/lib/docroutes.mjs` is the one
  answer to "which routes does this site publish."
- **Any page or navigation change** regenerates the content inventory
  (`npm run inventory:write` in `docs/site`).
- **`docs/site/src/content/docs/atlas/feature-matrix.md` is generated** from
  `docs/site/scripts/data/feature-matrix-rows.json`; edit the data and run
  `node docs/site/scripts/build-feature-matrix.mjs`. A `note` is capped at 200
  characters and is what the reader sees; `evidence` is recorded, not
  rendered. When a row's verdict flips, re-tense the old reasoning as history
  rather than deleting the measurement it carries
  (`check-matrix-verdict-prose.mjs`). A note must not restate a count or list
  the code or a generated table owns.
- **A page declares what it owns** with `owns:` in its frontmatter, and
  `scripts/check-feature-inventory.sh --write` regenerates the derived register
  `docs/feature-inventory.json`. The gate compares by exact string, never
  substring, and the claimed-row floor is a source constant rather than a field
  in the file (stokaro/ptah#2402). A column is named for what the gate checks
  -- `claimed_by`, `claimed`, `claimed_floor` -- never for what it suggests, and
  the word canonical appears nowhere, because the gate proves a claim resolves
  to a derived feature and no second page makes it, not that the page explains
  anything. Program rows come from `.goreleaser.yaml` `builds[].binary`, never
  from `go list`, which cannot know what ships.
- **A quick start** opts in with `quickstart: true`, and every command it
  publishes runs in CI on three platforms (`internal/quickstart` reads the page;
  `scripts/check-quickstart.sh` runs it). Section 8 of the style guide is the
  shape it reads; an output block naming no stream, or an `sql` block naming no
  file, is refused rather than skipped. The inference quick start needs
  PostgreSQL, pgvector and an HTTP provider, so it has its own Compose fixture
  and `docs/site/scripts/check-inference-quick-start.sh`; against a remote
  Docker context the CLI must reach that host, so run it as
  `PTAH_DOCKER_CONTEXT=<context> PTAH_FIXTURE_HOST=<host> docs/site/scripts/check-inference-quick-start.sh`.
- **Support pages stay separate**: `support-matrix.md` is generated,
  `support-policy.md` is the promise, `support-evidence.md` is measurement.
  Never move generated counts onto the latter two.
- **Diagrams** are semantic SVG, Mermaid, D2 or Graphviz source; PNGs are
  reserved for real browser UI regenerated from fixtures
  (`check-visual-assets.mjs`). After a visual fixture or generator changes,
  `PTAH_BIN=../../bin/ptah npm run assets:write` from `docs/site`.
  `check-generated-assets.mjs` byte-compares only the text samples under
  `docs/site/public/samples`; nothing compares the PNGs under
  `docs/site/src/assets/`, so look at them after `assets:write` before
  committing.
- **Examples**: every top-level `examples/*` carries the style guide's
  seven-section contract; `npm run examples:write` regenerates the index, and
  `npm run check:examples` plus `scripts/check-examples.sh` verify it.

For deep documentation maintenance use
`.agents/skills/ptah-documentation-maintenance/SKILL.md`.

### Label every issue you file

There are no issue templates, so a bare `gh issue create` produces an
unlabeled issue invisible to every planning filter. One type label --
`bug`, `enhancement`, `feature-request`, `documentation`, `question` -- and
area labels only where clear: `cli`, `migration`, `schema-generation`,
`postgresql`, `sql`, `rls`, `roles`, `security`, `foreign-key`,
`constraints`, `field-ordering`; `post-ga` and `critical` when they apply.
Engine names have no label; classify by what the issue is about. Milestones are
not used. Prefer no label to a wrong one.

`frozen` means work must not proceed and the comments say why; it is not
ownership, and a reason that no longer holds makes the label stale rather than
binding -- say so on the issue rather than deciding alone. `hold-off-merge`
means what it says.

## Code Style And Linting

`.golangci.yml` is a strict contract: fix code rather than relaxing thresholds
or broadening exclusions. Run both passes, `golangci-lint run --fix ./...` then
`golangci-lint run ./...`, and clean the second-pass fallout by hand.

Ptah is pre-GA: prefer the cleaner architecture over an alias, wrapper or
fallback kept for an older internal shape, and update callers, tests and docs.
The `ptah` binary stays purely native.

`modernize` is enabled: `slices.Contains`, `maps.Copy`, `strings.CutPrefix`,
`any`, `fmt.Fprintf(&b, ...)`, and `new(expr)` for an address rather than a
named temporary. No pointer helper packages. `importas` requires quicktest as
`qt`. A suppression names its rule -- `#nosec Gxxx -- reason` or
`//revive:disable... reason`, never a bare `//nolint:gosec` -- and every one
carries a justification; `nolintguard` runs through `go vet -vettool` in both
build-tag contours.

**A closing parenthesis goes on its own line** when the arguments began on
lines of their own. No linter reports the other shape; the cost is a diff that
touches a line nobody meant to change. Re-read the diff after any `gofmt -r`
rewrite, which re-wraps what it touches.

**Discover the module list, never write it out.** `scripts/list-go-modules.sh`
is the answer, from `git ls-files`; `make lint` consumes it, and
`scripts/check-go-module-lint-coverage.sh` fails when a tracked module is
missing from any workflow job that lints. Any "we do X for every module" claim
needs discovery or a check.

Every package carries a `// Package <name>` comment that says what it does and
where it sits, grounded in the code (`ST1000`); filler is not acceptable.

### Public API doc comments and examples

The ledger packages are read through godoc by embedders who never open this
repository, so the doc comment states what a caller must know: error semantics
and sentinels, zero-value and nil behavior, ordering and determinism, mutation
versus copy, accepted dialects. **Document stable caller-observable behavior,
not incidental implementation behavior.** Before writing a sentence ask: if we
changed the internals tomorrow while preserving the intended API, would this
sentence stop us? If so, generalize it; where the guarantee is intended,
state it precisely. `scripts/check-exported-docs.sh` measures presence only.

Every important usage pattern has an executable example in `example_test.go`,
in the shape `core/astbuilder/example_test.go` establishes: black-box,
attached to its symbol by name, deterministic `// Output:` on offline fixtures,
with a doc comment saying what it demonstrates. Not one `ExampleFoo` per
symbol. A compile-only example (no `// Output:`) is the fallback when
determinism is impossible, and its doc comment says why it does not run.
Examples are documentation, so idiomatic `if err != nil` is right there and
`scripts/check-test-style.sh` skips them; `must.Must` from
`github.com/go-extras/go-kit/must` is for fixture setup whose failure is not
the point, not the default spelling for brevity, and the call the example
demonstrates keeps its `if err != nil`.

## Testing Standards

### Where tests live

A test that crosses no process, filesystem, network or database boundary is a
unit or pipeline test and lives beside the production package. Every
integration test lives under an `integration/` tree, carries `//go:build
integration`, and is black-box: `package <name>_test`, exported API only,
never `*_internal_test.go` there. An integration tree holds nothing but tagged
test files; library code they use lives outside it, beside its own unit tests.

When a behavior is reachable only through an unexported symbol, move the
deterministic logic behind a package boundary and cover it with a black-box
unit test outside the integration trees, or introduce the real public boundary
the integration test should exercise. Never export a test-only accessor or use
white-box access as an integration shortcut.

`internal/testcontour` refuses a tagged test outside those trees, and CI runs
the whole contour through `go run ./internal/cmd/testcontour --tags
integration`, which fails on any skipped test -- a skip reads as a pass to
ordinary `go test`. Do not add build tags, package selectors or name patterns
to select integration tests.

**A live test asks for an engine, never for a variable.** `internal/dbtarget`
is the one declaration of which variable names which database:
`dbtarget.URL(c, dbtarget.PostgreSQL)` for the address ptah connects with,
`dbtarget.DriverDSN(c, dbtarget.MySQL)` for the form a raw driver parses. They
are not interchangeable -- `go-sql-driver/mysql` reads a `mysql://` prefix as
part of the username and reports access denied. Never read `os.Getenv` for a
database address in a test;
`TestNoIntegrationTestReadsARegistryVariableDirectly` enforces it
(stokaro/ptah#1541).

**Name the file for what the test needs**: `*_live_test.go` needs a server,
`*_e2e_test.go` drives a command or process (the larger claim, when both),
`*_fs_test.go` crosses only the filesystem. `*_integration_test.go` means
neither and is not used in new files. Classify by reading, not by scanning
imports: the helper that opens the database usually lives in a sibling file.

The Docker suite in `cmd/integration-test` covers apply, rollback, idempotency,
parallel execution, partial-failure recovery and schema diff.

### Declarative tests only

No `if`, `switch` or `goto` in a test function. `for` over a static table is
fine. `scripts/check-test-style.sh` holds a baseline of existing violations
(stokaro/ptah#541); new tests add no entries, and a cleanup refreshes it with
`--write-baseline` through the script, never the bare tool, which walks into
linked worktrees. `Example*` functions are exempt.

Never use `testify`; `depguard` refuses the import. Use `quicktest` as `qt`.

### Checkers belong to the test they report against

`QTLINT_RULES` in the Makefile enforces all three, in both build-tag contours
and again on a Windows runner, since a `_windows_test.go` file is outside both
Ubuntu contours. That job (`ptah-go-lint-windows`) runs `make lint-qtlint` and
golangci-lint rather than restating their arguments, and stays separate from
`windows-unit-tests`: a runtime failure must not hide the lint result, and a
lint-only rerun must not spend a unit-test budget. The three rules:

- Assert through a receiver: `c := qt.New(t)` then `c.Assert(...)`.
- Enter a subtest with `t.Run(name, func(t *testing.T) { c := qt.New(t) ...
  })`, never `c.Run`: a `c.Run` closure asserts through the parent's checker.
  The parameter is named `t` and shadows the outer one on purpose.
- A helper takes the checker: `func writeFixture(c *qt.C)`. `*qt.C` is a
  `testing.TB`, so widening to `testing.TB` buys nothing and hides the escape
  from `-require-testing-run`. A helper needing a concrete `*testing.T` takes
  one; it never asserts `c.TB.(*testing.T)`.

`make lint-qtlint-fix` applies the rewrites, one rule per pass.

### A table row carries data, not a checker

Put the value that varies in the row (`wantErr: "..."`), not a closure that
asserts. Rows whose assertions differ are two tests wearing one table -- split
them. Callbacks passed as inputs to production code are allowed; table rows
must not contain assertion callbacks, checker factories, or comparators that
select or implement the test's assertion strategy. `-require-data-rows`
reports only a function field taking `*qt.C`, so a `func() qt.Checker` or a
comparator without that parameter escapes it and is held by this sentence.

Do not hide the conditional in a helper either: no `checkError(c, err, wantIs,
wantLike)` choosing between `qt.ErrorIs`, `qt.ErrorMatches` and `qt.IsNil`.
Write the assertions per case.

**A rewritten table that still passes proves nothing.** Disable the defect its
rows exist to catch, watch the table redden, then restore it. A conversion that
drops a row's discrimination is worse than the shape it replaced.

### Separate happy-path and failure-path tests

`TestXxx_HappyPath` asserts `err, qt.IsNil` and the value; `TestXxx_FailurePath`
asserts the error and the zero value. Never one table with a `wantErr` branch.
Prefer `qt.ErrorIs` for a sentinel, `qt.ErrorAs` for a type, `qt.ErrorMatches`
when no sentinel exists.

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

Tests are `package <name>_test` exercising exported API. A white-box unit test
is permitted only for an unexported function critical to correctness or
internal state unobservable otherwise; it is named `*_internal_test.go` and
carries `// White-box testing required:` as the first non-empty line after
`package`, stating the justification. Never in an integration tree.

### A linter's reach is configuration, not a property of the linter

`rowserrcheck` tracks rows by where the `Query` that produced them was
declared, and its default list is `database/sql` alone -- so over Ptah's
readers, which hold a `sqlrunner.Runner`, it reported nothing while eleven
loops never asked `rows.Err()` (stokaro/ptah#2720). `.golangci.yml` names every
package declaring a `Query` that returns `*sql.Rows`, and
`internal/rowserrguard` compares that list with the tree in both directions.
The linter does not follow rows into a closure or through a parameter; a
function that runs its own `QueryContext` owns the terminal check.

A sweep hit is not a finding: `Rows.Err` reads a field only `Rows.Next` writes, so on
a result set nobody advanced it is nil however the statement fared, and a
check added there reads as handling and can never fire -- verify each site
against what the code does before adding one. Run a new linter against the
pre-fix tree before trusting a clean result.

### A blank import says why

Each blank import carries its own comment on its line or directly above it;
a comment heading a group does not count, because `gofmt` interleaves the
group. revive's `blank-imports` covers non-test code and
`internal/blankimportguard` covers the test files it exempts -- where the
imports matter most, since a registry test with a missing import shrinks what
it checks and still passes.

### A rule with no caller is a rule that is not in effect

`unused` counts a test as a use, so a function carrying a rule and its own
tests, called from nothing else, is green everywhere while the behavior is
absent (stokaro/ptah#2391, #2441). `internal/embedguard` reports the shape over
`internal/embed...`. A new rule needs a test the production path fails without
-- disable the rule and watch a test that drives the real entry point redden --
and a guard finding is a decision, not a suppression: either something should
call it, or the declaration should go.
