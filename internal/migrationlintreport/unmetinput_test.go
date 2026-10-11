package migrationlintreport_test

import (
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/migrationlintreport"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// unmetInputAnalysis returns an analysis of a column rename with the baseline
// supplied or withheld.
//
// Withheld is the state a run reaches when the replay read no columns for the
// version a rule asked about, which is what `lintDirectory` returns when the
// second pass has nothing to re-read with.
func unmetInputAnalysis(c *qt.C, baseline []lintext.BaselineColumn) lint.Analysis {
	c.Helper()
	analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fstest.MapFS{
		"1_base.sql":   {Data: []byte("CREATE TABLE users (id int NOT NULL);")},
		"2_rename.sql": {Data: []byte("ALTER TABLE users RENAME COLUMN id TO oid;")},
	}, lint.Options{
		Compatibility: lint.CompatibilityProfileAtlas,
		Dialect:       "postgres",
		DirFormat:     migrationfile.DirFormatAtlas,
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
		Analysis: unmetInputAnalysis(c, []lintext.BaselineColumn{{
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

// A statement an owner declined to judge on a run that names no dialect is
// named, since the clean report says nothing about it; a run whose every
// statement was judged writes nothing.
func TestWriteUnmetInputNoticeNamesUnjudgedStatements(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
		want string
	}{
		{name: "a drop YQL reads inside a string", sql: "SELECT 'a\\';\nDROP TOPIC t;\nSELECT '';\nDROP TOPIC u;\nSELECT '';\n",
			want: "warning: the ydb owner did not judge 1_t.sql:2, 1_t.sql:4: its own lexing splits the text differently " +
				"than the dialect-blind scanner this run used, so the rules that read what only ydb recognizes did not " +
				"check them; name the dialect to read the text one way\n"},
		{name: "every drop judged", sql: "DROP TOPIC t;\n", want: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fstest.MapFS{
				"1_t.sql": {Data: []byte(test.sql)},
			}, lint.Options{DirFormat: migrationfile.DirFormatAtlas})
			c.Assert(err, qt.IsNil)
			var out strings.Builder

			err = migrationlintreport.WriteUnmetInputNotice(&out, migrationlintreport.Report{Analysis: analysis})

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}
