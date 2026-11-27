# Test design policy

Repository-specific requirements for the matching task. Paths in code spans
are relative to the repository root. Read only the sections the task needs.

## Contents

- [Testing Standards](#testing-standards)
- [Where tests live](#where-tests-live)
- [An engine-observable change carries an engine-level test](#an-engine-observable-change-carries-an-engine-level-test)
- [Every fix carries a regression test](#every-fix-carries-a-regression-test)
- [Declarative tests only](#declarative-tests-only)
- [Checkers belong to the test they report against](#checkers-belong-to-the-test-they-report-against)
- [A table row carries data, not a checker](#a-table-row-carries-data-not-a-checker)
- [Separate happy-path and failure-path tests](#separate-happy-path-and-failure-path-tests)
- [Black-box by default, white-box as the exception](#black-box-by-default-white-box-as-the-exception)
- [A linter's reach is configuration, not a property of the linter](#a-linters-reach-is-configuration-not-a-property-of-the-linter)
- [A blank import says why](#a-blank-import-says-why)
- [A rule with no caller is a rule that is not in effect](#a-rule-with-no-caller-is-a-rule-that-is-not-in-effect)

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
- `internal/testcontour` refuses tagged tests elsewhere and fails on any skip.
  CI splits the contour across shards, and `testcontour` alone decides the
  split: each shard lists every test with `go test -list`, partitions the
  listing and runs its part, and the `integration-tests` job (`testcontour
  verify`) proves from the shards' reports that their union is the listing,
  above a floor, with every test run once and none skipped or failed. Nothing
  else selects membership of the complete integration contour. A focused
  tagged run is local diagnostic evidence only and cannot replace that proof.
- **A test removes what it creates in a shared database**, a revision table's
  companions included (`revisiontable.Configured`, plus
  `revisiontable.ApplyLog` for `schema apply`). A shard runs part of the
  contour, and the timings move the partition, so a leftover turns a different
  test red on each run, far from the test that left it. `Configured` also
  names `ptah_migration_tags`, which every revision table shares. Dropping it
  after a test is safe only because the contour runs one test at a time
  (`-p=1`, and no `t.Parallel` in a package that migrates).
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
tested there. The end-to-end, live and parity tests of DuckDB, Spanner and YDB
run in their plugin repositories against current Ptah and never join
`go-integration-tests.yml` (ADR 0031).

### Every fix carries a regression test

A behavioral fix lands with a test that fails without it and passes with it.
The test drives the path the defect took: the same verb, selector, input shape
and, where the defect needed one, engine. Prove it: disable the fix, watch the
test fail, restore it. When the existing tests missed the defect, the pull
request names the shape they lacked, and that shape becomes a row.

### Declarative tests only

No `if`, `switch` or `goto` in a test function; `for` over a static table is
fine; `Example*` is exempt. New tests add nothing to the
`scripts/check-test-style.sh` baseline, refreshed only through the script's
`--write-baseline`, never the bare tool. Never use `testify` (`depguard` refuses
it); `importas` requires `quicktest` as `qt`.

### Checkers belong to the test they report against

`QTLINT_RULES` in the Makefile enforces these in both contours and on Windows;
`make lint-qtlint-fix` rewrites, one rule per pass.

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
		got, err := atlasurl.DialectFromURL("db2://localhost/dev")
		c.Assert(err, qt.ErrorMatches, `unsupported --dev-url dialect "db2://localhost/dev"`)
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
