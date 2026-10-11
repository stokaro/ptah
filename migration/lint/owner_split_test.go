package lint_test

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/lintext"
	"ptah.run/core/ptaherr"
	"ptah.run/engine"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
)

// recordingSplitter answers every split with the statements it holds and
// records each request.
type recordingSplitter struct {
	requests   *[]lintext.SplitRequest
	statements []lintext.Statement
}

func (s recordingSplitter) SplitStatements(_ context.Context, request lintext.SplitRequest) (lintext.SplitResult, error) {
	*s.requests = append(*s.requests, request)
	result := lintext.SplitResult{}
	for range request.Sources {
		result.Files = append(result.Files, s.statements)
	}
	return result, nil
}

// ownerRuntime registers postgres with a splitter that answers with
// statements for every file.
func ownerRuntime(c *qt.C, requests *[]lintext.SplitRequest, statements ...lintext.Statement) *engine.Runtime {
	c.Helper()
	return must.Must(engine.New(engine.Provider{ID: "example.org/postgres", Targets: []engine.Target{{Name: "postgres",
		Splitter: recordingSplitter{requests: requests, statements: statements}}}}))
}

// dropMigration is a directory whose one up file drops a table.
func dropMigration() map[string]string {
	return map[string]string{
		"0000000001_drop.up.sql":   "DROP TABLE users;\n",
		"0000000001_drop.down.sql": "CREATE TABLE users (id int);\n",
	}
}

func ruleCodes(findings []lint.Finding) []string {
	codes := make([]string, 0, len(findings))
	for _, finding := range findings {
		codes = append(codes, finding.Rule)
	}
	return slices.Compact(codes)
}

// A run that names a target reads the statements its owner splits, in one
// call for every file, each named with its version and its half: an owner
// that answers the drop as no statement at all leaves the data-safety rule
// nothing to find, which the bundled splitter finds.
func TestLintFS_ReadsTheOwnersSplit(t *testing.T) {
	c := qt.New(t)
	var requests []lintext.SplitRequest

	silent, err := lint.LintFS(c.Context(), ownerRuntime(c, &requests), fixture(dropMigration()), lint.Options{Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	bundled, err := lint.LintFS(c.Context(), builtintest.Runtime(), fixture(dropMigration()), lint.Options{Dialect: "postgres"})
	c.Assert(err, qt.IsNil)

	c.Assert(ruleCodes(silent), qt.Not(qt.Contains), "DS101")
	c.Assert(ruleCodes(bundled), qt.Contains, "DS101")
	c.Assert(requests, qt.DeepEquals, []lintext.SplitRequest{{Target: "postgres", Sources: []lintext.Source{
		{Name: "0000000001_drop.down.sql", SQL: "CREATE TABLE users (id int);\n", Version: 1, Down: true},
		{Name: "0000000001_drop.up.sql", SQL: "DROP TABLE users;\n", Version: 1, Up: true},
	}}})
}

// The owner's statement forms are what rules read: a statement the owner
// spells as a drop is one, whatever the file says.
func TestLintFS_ReadsTheOwnersWords(t *testing.T) {
	c := qt.New(t)
	var requests []lintext.SplitRequest
	runtime := ownerRuntime(c, &requests, lintext.Statement{Start: 0, End: 4, Line: 1, SQL: "DROP",
		Canonical: "DROP TABLE users", Words: []string{"DROP", "TABLE", "users"}, SourceWords: []string{"DROP", "TABLE", "users"}})

	findings, err := lint.LintFS(c.Context(), runtime, fixture(map[string]string{
		"0000000001_drop.up.sql":   "DROP something the owner reads as a table drop;\n",
		"0000000001_drop.down.sql": "DROP TABLE users;\n",
	}), lint.Options{Dialect: "postgres"})

	c.Assert(err, qt.IsNil)
	c.Assert(ruleCodes(findings), qt.Contains, "DS101")
}

// A run that names no target splits with the dialect-blind scanner and asks
// no owner.
func TestLintFS_SplitsAnUnnamedTargetItself(t *testing.T) {
	c := qt.New(t)
	var requests []lintext.SplitRequest

	findings, err := lint.LintFS(c.Context(), ownerRuntime(c, &requests), fixture(dropMigration()), lint.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(ruleCodes(findings), qt.Contains, "DS101")
	c.Assert(requests, qt.HasLen, 0)
}

// A run that names a target its runtime holds no splitter for is refused,
// rather than split some other way.
func TestLintFS_OwnerSplit_FailurePath(t *testing.T) {
	c := qt.New(t)
	without := must.Must(engine.New(engine.Provider{ID: "example.org/postgres", Targets: []engine.Target{{Name: "postgres"}}}))

	findings, err := lint.LintFS(c.Context(), without, fixture(dropMigration()), lint.Options{Dialect: "postgres"})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(err, qt.ErrorMatches, `split the statements for postgres: .*no statement splitter for target "postgres"`)
	c.Assert(findings, qt.IsNil)
}

// A run needs a runtime whatever it names.
func TestLintFS_NoRuntime_FailurePath(t *testing.T) {
	c := qt.New(t)

	findings, err := lint.LintFS(c.Context(), nil, fixture(dropMigration()), lint.Options{})

	c.Assert(err, qt.ErrorMatches, `lint analysis requires a context and an engine runtime`)
	c.Assert(findings, qt.IsNil)
}

// The contract's levels are this package's, spelled the same, so a level an
// owner publishes converts by value.
func TestSeverity_MatchesTheContract(t *testing.T) {
	c := qt.New(t)
	c.Assert([]lint.Severity{lint.Severity(lintext.Info), lint.Severity(lintext.Warning), lint.Severity(lintext.Error)}, qt.DeepEquals,
		[]lint.Severity{lint.SeverityInfo, lint.SeverityWarning, lint.SeverityError})
}
