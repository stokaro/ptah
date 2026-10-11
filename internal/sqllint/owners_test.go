package sqllint_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/sqlext"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqllint"
)

// findingRules lists the rule of each finding.
func findingRules(findings []sqllint.Finding) []string {
	rules := make([]string, 0, len(findings))
	for _, finding := range findings {
		rules = append(rules, finding.Rule)
	}
	return rules
}

// A file is read with the owners whose syntax it may carry, so a SQLite
// virtual table is read as the SQLite owner reads it rather than reported as
// SQL the linter cannot read; with the shared grammar alone it is.
func TestLintSource_ReadsTheOwnersSyntax(t *testing.T) {
	const virtual = "CREATE VIRTUAL TABLE docs USING fts5(title, body);"
	for _, test := range []struct {
		name    string
		owners  sqlext.Set
		wantSQL bool
	}{
		{name: "the runtime's owners", owners: builtintest.Runtime().SQL()},
		{name: "the shared grammar alone", owners: sqlext.None(), wantSQL: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings, err := sqllint.LintSource(c.Context(), sqllint.Source{Name: "docs.sql", SQL: virtual},
				sqllint.Options{SQL: test.owners, Bodies: builtintest.Runtime(), Dialect: platform.SQLite})

			c.Assert(err, qt.IsNil)
			c.Assert(slices.Contains(findingRules(findings), "SQL002"), qt.Equals, test.wantSQL)
		})
	}
}

// A set nobody selected is refused, so a caller that forgot the owners does
// not read every owner's syntax as SQL the linter cannot read.
func TestLintSource_RefusesAnUnselectedOwnerSet(t *testing.T) {
	c := qt.New(t)

	findings, err := sqllint.LintSource(c.Context(), sqllint.Source{Name: "docs.sql", SQL: "SELECT 1;"},
		sqllint.Options{Bodies: builtintest.Runtime(), Dialect: platform.SQLite})

	c.Assert(err, qt.ErrorIs, sqlext.ErrUnselected)
	c.Assert(findings, qt.IsNil)
}
