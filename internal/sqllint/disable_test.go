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
			name:     "parse error, its own code disabled",
			sql:      "CREATE TABLE ;",
			disabled: []string{"SQL001"},
			want:     make([]string, 0),
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
			name:     "statement the linter does not model, its own code disabled",
			sql:      "SELECT 1;",
			disabled: []string{"SQL002"},
			want:     make([]string, 0),
		},
		{
			name:     "both parse-path codes, the family disabled",
			sql:      "CREATE TABLE ;\nSELECT 1;",
			disabled: []string{"SQL"},
			want:     make([]string, 0),
		},
		{
			name:     "both parse-path codes, one of them disabled",
			sql:      "CREATE TABLE ;\nSELECT 1;",
			disabled: []string{"SQL001"},
			want:     []string{"SQL002"},
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
