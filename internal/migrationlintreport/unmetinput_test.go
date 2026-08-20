package migrationlintreport_test

import (
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migrationlintreport"
	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
)

// unmetInputAnalysis returns an analysis of a column rename with the baseline
// supplied or withheld.
//
// Withheld is the state a run reaches when the replay read no columns for the
// version a rule asked about, which is what `lintDirectory` returns when the
// second pass has nothing to re-read with.
func unmetInputAnalysis(c *qt.C, baseline []lint.BaselineColumn) lint.Analysis {
	c.Helper()
	analysis, err := lint.AnalyzeFS(fstest.MapFS{
		"1_base.sql":   {Data: []byte("CREATE TABLE users (id int NOT NULL);")},
		"2_rename.sql": {Data: []byte("ALTER TABLE users RENAME COLUMN id TO oid;")},
	}, lint.Options{
		Compatibility: lint.CompatibilityProfileAtlas,
		Dialect:       "postgres",
		DirFormat:     migrator.MigrationDirFormatAtlas,
		Selection:     lint.VersionSelection{Versions: []int64{2}, Restricted: true},
		Baseline:      baseline,
	})
	c.Assert(err, qt.IsNil)
	return analysis
}

// TestWriteUnmetInputNoticeNamesTheRule is the fact a reader gets instead of
// silence.
//
// The run exits 0 and prints a clean report either way, so the notice is the
// only thing distinguishing "nothing to report" from "the analyzer that would
// have reported it never got its input" (stokaro/ptah#1632).
func TestWriteUnmetInputNoticeNamesTheRule(t *testing.T) {
	c := qt.New(t)
	var out strings.Builder

	err := migrationlintreport.WriteUnmetInputNotice(&out, migrationlintreport.Report{
		Analysis: unmetInputAnalysis(c, nil),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "DD101")
	c.Assert(out.String(), qt.Contains, "baseline schema")
	c.Assert(out.String(), qt.Contains, "thinner")
	// One line, so a log reader sees a notice rather than a wall.
	c.Assert(strings.Count(out.String(), "\n"), qt.Equals, 1)
}

// TestWriteUnmetInputNoticeIsSilentWhenTheInputArrived is the control that
// fails if the notice ever became unconditional — which would train a reader to
// ignore it, and would be worse than not having it.
func TestWriteUnmetInputNoticeIsSilentWhenTheInputArrived(t *testing.T) {
	c := qt.New(t)
	var out strings.Builder

	err := migrationlintreport.WriteUnmetInputNotice(&out, migrationlintreport.Report{
		Analysis: unmetInputAnalysis(c, []lint.BaselineColumn{{
			Version:  2,
			Table:    "users",
			Name:     "id",
			DataType: "integer",
			NotNull:  true,
		}}),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "")
}

// TestWriteUnmetInputNoticeIsSilentForAnEmptyReport covers the caller that has
// no analysis at all — a run that failed before analyzing must not have a
// notice appended to its error.
func TestWriteUnmetInputNoticeIsSilentForAnEmptyReport(t *testing.T) {
	c := qt.New(t)
	var out strings.Builder

	err := migrationlintreport.WriteUnmetInputNotice(&out, migrationlintreport.Report{})

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "")
}
