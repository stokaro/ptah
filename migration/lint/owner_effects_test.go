package lint_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/lintext"
	"ptah.run/dialect/ydb/ydblint"
	"ptah.run/engine"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/lintscan"
	"ptah.run/migration/lint"
)

// A run that names no dialect still reports a dropped topic: the YDB owner
// judges the drop over the dialect-blind split, and that judgment does not
// depend on the YD family running, so disabling it leaves DS107 in place.
func TestLintFS_NoDialect_DS107ReadsTheOwnersDrops(t *testing.T) {
	for _, test := range []struct {
		name     string
		disabled []string
	}{
		{name: "every rule"},
		{name: "the YD family disabled", disabled: []string{"YD"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
				"0001_t.up.sql": "CREATE TABLE notes (id int);\nDROP TOPIC t;\n",
			}), lint.Options{Disabled: test.disabled})

			c.Assert(err, qt.IsNil)
			c.Assert(dsSites(findingSites(findings)), qt.DeepEquals, []string{"0001_t.up.sql:2:DS107"})
		})
	}
}

// countingAnnotator counts the calls an annotator answers.
type countingAnnotator struct {
	calls *int
	lintext.Annotator
}

func (a countingAnnotator) AnnotateStatements(ctx context.Context, request lintext.AnnotateRequest) (lintext.AnnotateResult, error) {
	*a.calls++
	return a.Annotator.AnnotateStatements(ctx, request)
}

// annotatedRuntime registers postgres with the bundled splitter, and ydb with
// nothing but the YDB owner's annotator, counting its calls.
func annotatedRuntime(c *qt.C, calls *int) *engine.Runtime {
	c.Helper()
	return must.Must(engine.New(
		engine.Provider{ID: "example.org/postgres", Targets: []engine.Target{{Name: "postgres",
			Splitter: lintscan.Splitter{Dialect: "postgres"}}}},
		engine.Provider{ID: "example.org/ydb", Targets: []engine.Target{{Name: "ydb",
			Annotator: countingAnnotator{calls: calls, Annotator: ydblint.Annotator{}}}}},
	))
}

// The annotation is the no-dialect run's: one call for every file of the run,
// and none on a run that names postgres, which reads a dropped topic the way
// its own owner splits it and so reports nothing about it.
func TestLintFS_AnnotatesOnlyARunThatNamesNoDialect(t *testing.T) {
	for _, test := range []struct {
		name      string
		dialect   string
		wantCalls int
		wantSites []string
	}{
		{name: "no dialect", wantCalls: 1, wantSites: []string{"0001_t.up.sql:1:DS107", "0002_u.up.sql:1:DS107"}},
		{name: "postgres", dialect: "postgres", wantCalls: 0, wantSites: make([]string, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0

			findings, err := lint.LintFS(c.Context(), annotatedRuntime(c, &calls), fixture(map[string]string{
				"0001_t.up.sql": "DROP TOPIC t;\n",
				"0002_u.up.sql": "DROP TRANSFER u;\n",
			}), lint.Options{Dialect: test.dialect})

			c.Assert(err, qt.IsNil)
			c.Assert(calls, qt.Equals, test.wantCalls)
			c.Assert(dsSites(findingSites(findings)), qt.DeepEquals, test.wantSites)
		})
	}
}

// A statement the YDB owner splits differently is not judged, and the
// analysis names it rather than reading it as a statement that drops nothing:
// the dialect-blind scanner ends the first string at the backslash, while YQL
// reads the backslash as an escape and the DROP TOPIC as part of the string.
// A file the run does not select is not named, as it reports no finding.
func TestAnalyzeFS_NoDialect_NamesWhatAnOwnerDeclined(t *testing.T) {
	for _, test := range []struct {
		name      string
		selection lint.VersionSelection
		want      []lint.UnjudgedStatement
		wantSites []string
	}{
		{name: "every file", want: []lint.UnjudgedStatement{{Target: "ydb", File: "0001_t.up.sql", Line: 2}},
			wantSites: []string{"0002_u.up.sql:1:DS107"}},
		{name: "the second file selected", selection: lint.VersionSelection{Versions: []int64{2}, Restricted: true},
			want: nil, wantSites: []string{"0002_u.up.sql:1:DS107"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(map[string]string{
				"0001_t.up.sql": "SELECT 'a\\';\nDROP TOPIC t;\nSELECT '';\n",
				"0002_u.up.sql": "DROP TOPIC u;\n",
			}), lint.Options{Selection: test.selection})

			c.Assert(err, qt.IsNil)
			c.Assert(analysis.UnjudgedStatements(), qt.DeepEquals, test.want)
			c.Assert(dsSites(findingSites(analysis.Findings())), qt.DeepEquals, test.wantSites)
		})
	}
}

// judgingSplitter is the bundled postgres splitter, reporting that it judged
// every fact while reporting no effect.
type judgingSplitter struct{}

func (judgingSplitter) SplitStatements(ctx context.Context, request lintext.SplitRequest) (lintext.SplitResult, error) {
	result, err := lintscan.Splitter{Dialect: "postgres"}.SplitStatements(ctx, request)
	result.Judged = []lintext.Fact{lintext.FactStatefulDrop, lintext.FactUniqueIndex, lintext.FactRebuildDrop, lintext.FactRebuildRename}
	return result, err
}

// Where a split judged the rebuild facts, the owner's answer replaces the
// host's own match: an owner that reports no rebuild leaves the drop that ends
// one a drop, which the bundled splitter, judging none, passes over.
func TestLintFS_JudgedRebuildsAreTheOwnersAnswer(t *testing.T) {
	const rebuild = "CREATE TABLE items_new (id int, n int);\n" +
		"INSERT INTO items_new (id, n) SELECT id, n FROM items;\n" +
		"DROP TABLE items;\n" +
		"ALTER TABLE items_new RENAME TO items;\n"
	judging := must.Must(engine.New(engine.Provider{ID: "example.org/postgres", Targets: []engine.Target{{Name: "postgres",
		Splitter: judgingSplitter{}}}}))
	for _, test := range []struct {
		name    string
		runtime *engine.Runtime
		want    []string
	}{
		{name: "judged by the owner", runtime: judging, want: []string{"0002_rebuild.up.sql:3:DS101"}},
		{name: "matched by the host", runtime: builtintest.Runtime(), want: make([]string, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings, err := lint.LintFS(c.Context(), test.runtime, fixture(map[string]string{
				"0001_items.up.sql":   "CREATE TABLE items (id int, n int);\n",
				"0002_rebuild.up.sql": rebuild,
			}), lint.Options{Dialect: "postgres"})

			c.Assert(err, qt.IsNil)
			c.Assert(dsSites(findingSites(findings)), qt.DeepEquals, test.want)
		})
	}
}
