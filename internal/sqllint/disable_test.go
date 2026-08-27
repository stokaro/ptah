package sqllint_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/sqllint"
)

// This file pins what `--disable` reaches.
//
// Two of the four identifiers are produced by the parse path, which runs before
// any rule object exists, so the per-rule check inside the statement loop never
// sees them. A selector naming SQL001 or SQL002 used to be accepted and
// silently ignored: the finding was still reported and `ptah sql lint` still
// exited 1. A flag that is accepted and does nothing is worse than a flag that
// refuses, and the reference page tells an operator this one works.

// reportedCodes lints one source and returns the identifiers that survived.
func reportedCodes(tb testing.TB, sql string, disabled []string) []string {
	c := qt.New(tb)

	findings, err := sqllint.LintSource(
		sqllint.Source{Name: "probe.sql", SQL: sql},
		sqllint.Options{Dialect: platform.Postgres, DisabledRules: disabled},
	)
	c.Assert(err, qt.IsNil)

	codes := make([]string, 0, len(findings))
	for _, finding := range findings {
		codes = append(codes, finding.Rule)
	}
	slices.Sort(codes)
	return slices.Compact(codes)
}

// TestDisableReachesEveryIdentifier walks each identifier against a selector
// that should silence it and a selector that should not. The second half is
// what separates a working filter from one that drops everything.
func TestDisableReachesEveryIdentifier(t *testing.T) {
	rows := []struct {
		name     string
		sql      string
		disabled []string
		want     []string
	}{
		{
			name: "parse error, nothing disabled",
			sql:  "CREATE TABLE ;",
			want: []string{"SQL001"},
		},
		{
			name:     "parse error, another rule's code disabled",
			sql:      "CREATE TABLE ;",
			disabled: []string{"DDL001"},
			want:     []string{"SQL001"},
		},
		{
			name: "statement the linter does not model, nothing disabled",
			sql:  "SELECT 1;",
			want: []string{"SQL002"},
		},
		{
			name: "table without a primary key, nothing disabled",
			sql:  "CREATE TABLE users (email TEXT NOT NULL);",
			want: []string{"DDL001"},
		},
		{
			name:     "table without a primary key, its own code disabled",
			sql:      "CREATE TABLE users (email TEXT NOT NULL);",
			disabled: []string{"DDL001"},
			want:     make([]string, 0),
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(reportedCodes(t, row.sql, row.disabled), qt.DeepEquals, row.want)
		})
	}
}

// TestDisableRefusesTheParsePathCodes pins the third option.
//
// SQL001 and SQL002 do not report an opinion about the SQL; they report that no
// opinion could be formed. Silencing one turns "I could not read this file"
// into a clean run at exit 0, which is what #1270 means by "Parse/analysis
// incompleteness is visible to the user".
func TestDisableRefusesTheParsePathCodes(t *testing.T) {
	rows := []struct {
		name     string
		disabled []string
		covers   string
	}{
		{name: "the parse error's own code", disabled: []string{"SQL001"}, covers: "SQL001"},
		{name: "the unmodeled statement's own code", disabled: []string{"SQL002"}, covers: "SQL002"},
		{name: "the family prefix, which covers both", disabled: []string{"SQL"}, covers: "SQL001"},
		{name: "lower case, since selectors are folded", disabled: []string{"sql001"}, covers: "SQL001"},
		{
			// Refused for the one selector that covers a parse-path code, even
			// when another selector in the same flag is fine.
			name: "beside a selector that is allowed", disabled: []string{"DDL001", "SQL002"}, covers: "SQL002",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := sqllint.LintSource(
				sqllint.Source{Name: "probe.sql", SQL: "CREATE TABLE ;"},
				sqllint.Options{Dialect: platform.Postgres, DisabledRules: row.disabled},
			)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, row.covers)
			c.Assert(err.Error(), qt.Contains, "could not be analyzed")
		})
	}
}

// TestDisableStillReachesAnOpinionRule is the control the refusal needs.
//
// Without it, refusing every selector would pass: a --disable that never works
// is the flag this file was written to prevent.
func TestDisableStillReachesAnOpinionRule(t *testing.T) {
	c := qt.New(t)

	c.Assert(reportedCodes(t, "CREATE TABLE users (email TEXT NOT NULL);", []string{"DDL001"}),
		qt.DeepEquals, make([]string, 0))
}
