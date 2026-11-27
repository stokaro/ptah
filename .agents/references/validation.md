# Build and validation policy

Repository-specific requirements for the matching task. Paths in code spans
are relative to the repository root. Read only the sections the task needs.

## Contents

- [Building And Testing](#building-and-testing)
- [Pull-request scoping](#pull-request-scoping)
- [The version matrix](#the-version-matrix)
- [Lint rules](#lint-rules)
- [The Go toolchain](#the-go-toolchain)

## Building And Testing

```bash
make build                       # ptah, ptah-ls, ptah-compat, ptah-integration-test
go test ./... -count=1           # unit contour
make lint                        # golangci-lint, qtlint, nolintguard -- three targets, all required
scripts/check-test-style.sh      # declarative-test baseline
make integration-test            # Docker Compose suite; binds fixed host ports
```

- These are full-run entry points. Use existing Go commands for focused
  diagnostics and `internal/cmd/testcontour` for the complete tagged suite;
  [TESTING.md](../../TESTING.md) distinguishes them. A wrapper stays thin,
  with no discovery, orchestration or reporting of its own.
- `make docker-clean` prunes the whole Docker daemon; do not use it for a
  task's cleanup. Check shared services before `make integration-test` and
  remove only resources the task owns.
- `make lint` is not the whole lint job (`.github/workflows/go-lint.yml` runs
  policy scripts too). Account for each applicable check from the workflow
  with local or current-head CI evidence. Report pending and blocked checks;
  do not treat a focused pass as a complete job. Use the validation skill's
  planning checkpoints to avoid repeating expensive local runs.

A gate that enumerates files uses `git ls-files`, not a filesystem walk that
can include parked checkouts. A generator must exit successfully, and a corpus
check must enforce a meaningful floor. An empty or partial run cannot pass.

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

### Lint rules

A lint rule is identified by its name, `category/name`, everywhere the lint
core reads one: findings, selectors, baselines, the apply gate and every native
report. Ptah rules have no numeric code. The categories are declared once in
`internal/lintnames`; the engine runtime accepts one name from several owners
when their dialects are disjoint and their severities match, so one hazard is
one rule with a check per engine.

Every rule name is enumerated on one generated page. Adding a rule is two
edits -- the rule, which declares its name, and its meaning in
`internal/lintcatalog` -- and `scripts/check-docsync.sh` fails when they
disagree. A selector is an exact name or an exact category, never a prefix, and
native code refuses any word spelled as a code, with a message that names no
rule: native code holds no table from codes to rules.

The Atlas check codes live in `internal/atlascodes`, which only the
compatibility surface (`ptah-compat`, `internal/atlasreport`) and the
documentation generator import. A native or shared package must not. The
table from names to Atlas codes and the reviewed reference snapshot are gated
there, in both directions. Every `ptah-compat` report prints a finding the
way the community binary does: the Atlas code, in that binary's report shape,
for a rule the table maps, and the rule name for one it does not, since there
is no code to print. `PTAH_ATLAS_STRICT_COMPAT=1` changes which rules run, not
how a finding is printed: it runs only the rules the table maps. `atlas:nolint`
reads a code or an analyzer name in both modes and a rule name in the default
mode only.

The rule-name grammar is published once by the provider SDK as
`provider.ValidLintRuleName`, because a provider is a separate module. The
host's `lintext.CanonicalName` is the host's copy, since `core/lintext` must
not link the SDK, and `internal/providerhost` holds the two to one answer.

### The Go toolchain

In `go.mod`, `go` is the published compatibility floor and moves on a human
decision; `toolchain` is the CI build and moves every patch release. Never
write a `go-version:` literal into a workflow (use `go-version-file: go.mod`)
or the version into `.golangci.yml`; clear a standard-library advisory by
raising `toolchain`, never `go`. `scripts/check-go-toolchain-single-source.sh`
enforces this; `.github/actions/ptah/action.yml` is its one exemption and
says why.

The Windows jobs that run Go tests patch the toolchain's runtime with the
approved, unmerged upstream fix for golang/go#81238 (`scripts/go-runtime-patch`,
stokaro/ptah#2365). The step skips a toolchain that carries the fix and fails
the job when the patch does not apply, so a Windows failure that names CL 828724
is about that step, not the change under test. `go-runtime-patch-watch.yml`
opens an issue when upstream moves.
