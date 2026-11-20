package sqllint_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqllint"
)

// This file pins what `--disable` reaches.
//
// Two of the four identifiers are produced by the parse path, which runs before
// any rule object exists, so the per-rule check inside the statement loop never
// sees them. A selector naming correctness/syntax-error or coverage/unsupported-statement that is accepted and silently
// ignored leaves the finding reported and `ptah sql lint` exiting 1. A flag
// that is accepted and does nothing is worse than a flag that refuses, and the
// reference page tells an operator this one works.

// reportedCodes lints one source and returns the identifiers that survived.
func reportedCodes(tb testing.TB, sql string, disabled []string) []string {
	c := qt.New(tb)

	findings, err := sqllint.LintSource(c.Context(),
		sqllint.Source{Name: "probe.sql", SQL: sql},
		sqllint.Options{SQL: builtintest.Runtime().SQL(), Bodies: builtintest.Runtime(), Dialect: platform.Postgres, DisabledRules: disabled},
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
			want: []string{"correctness/syntax-error"},
		},
		{
			name:     "parse error, another rule's code disabled",
			sql:      "CREATE TABLE ;",
			disabled: []string{"design/no-primary-key"},
			want:     []string{"correctness/syntax-error"},
		},
		{
			name: "statement the linter does not model, nothing disabled",
			sql:  "SELECT 1;",
			want: []string{"coverage/unsupported-statement"},
		},
		{
			name: "table without a primary key, nothing disabled",
			sql:  "CREATE TABLE users (email TEXT NOT NULL);",
			want: []string{"design/no-primary-key"},
		},
		{
			name:     "table without a primary key, its own code disabled",
			sql:      "CREATE TABLE users (email TEXT NOT NULL);",
			disabled: []string{"design/no-primary-key"},
			want:     make([]string, 0),
		},
		{
			name:     "table without a primary key, its name disabled",
			sql:      "CREATE TABLE users (email TEXT NOT NULL);",
			disabled: []string{"design/no-primary-key"},
			want:     make([]string, 0),
		},
		{
			name:     "table without a primary key, its category disabled",
			sql:      "CREATE TABLE users (email TEXT NOT NULL);",
			disabled: []string{"design"},
			want:     make([]string, 0),
		},
		{
			name:     "table without a primary key, its category disabled with a slash",
			sql:      "CREATE TABLE users (email TEXT NOT NULL);",
			disabled: []string{"design/"},
			want:     make([]string, 0),
		},
		{
			name:     "table without a primary key, another category disabled",
			sql:      "CREATE TABLE users (email TEXT NOT NULL);",
			disabled: []string{"naming"},
			want:     []string{"design/no-primary-key"},
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
// correctness/syntax-error and coverage/unsupported-statement do not report an opinion about the SQL; they report that no
// opinion could be formed. Silencing one turns "I could not read this file"
// into a clean run at exit 0, which is what #1270 means by "Parse/analysis
// incompleteness is visible to the user".
func TestDisableRefusesTheParsePathCodes(t *testing.T) {
	rows := []struct {
		name     string
		disabled []string
		covers   string
	}{
		{name: "the parse error's own code", disabled: []string{"correctness/syntax-error"}, covers: "correctness/syntax-error"},
		{name: "the unmodeled statement's own code", disabled: []string{"coverage/unsupported-statement"}, covers: "coverage/unsupported-statement"},
		{name: "the parse error's category", disabled: []string{"correctness"}, covers: "correctness/syntax-error"},
		{name: "the unmodeled statement's name", disabled: []string{"coverage/unsupported-statement"}, covers: "coverage/unsupported-statement"},
		{
			// Refused for the one selector that covers a parse-path code, even
			// when another selector in the same flag is fine.
			name: "beside a selector that is allowed", disabled: []string{"design/no-primary-key", "coverage/unsupported-statement"}, covers: "coverage/unsupported-statement",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := sqllint.LintSource(t.Context(),
				sqllint.Source{Name: "probe.sql", SQL: "CREATE TABLE ;"},
				sqllint.Options{SQL: builtintest.Runtime().SQL(), Bodies: builtintest.Runtime(), Dialect: platform.Postgres, DisabledRules: row.disabled},
			)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, row.covers)
			c.Assert(err.Error(), qt.Contains, "could not be analyzed")
		})
	}
}

// TestDisableRefusesAnUnknownSelector refuses a selector that names no rule,
// by the validation `migrations lint --disable` applies. Each of these
// matched nothing before, so the flag was accepted and the finding stayed.
func TestDisableRefusesAnUnknownSelector(t *testing.T) {
	rows := []struct {
		name     string
		disabled string
		wantErr  string
	}{
		{name: "a category in the wrong case", disabled: "Design", wantErr: `--disable "Design" is not a rule name or category.*`},
		{name: "a word", disabled: "foo", wantErr: `--disable "foo" is not a rule name or category.*`},
		{name: "a name and a code joined by a slash", disabled: "design/no-primary-key/DDL002", wantErr: `--disable "design/no-primary-key/DDL002" is not the name of a SQL lint rule.*`},
		{name: "a lower-case code", disabled: "ddl001", wantErr: `--disable "ddl001" is not a rule name or category.*`},
		{name: "a code", disabled: "DDL001", wantErr: `--disable lint rules are named category/name; DDL001 is not a rule name`},
		{name: "a code prefix", disabled: "SQL", wantErr: `--disable lint rules are named category/name; SQL is not a rule name`},
		{name: "a name no rule has, in a declared category", disabled: "design/zz999", wantErr: `--disable "design/zz999" is not the name of a SQL lint rule.*`},
		{name: "a name no rule has", disabled: "design/nothing", wantErr: `--disable "design/nothing" is not the name of a SQL lint rule; .*`},
		{name: "a category that is not declared", disabled: "desing/", wantErr: `--disable "desing/" is not a rule category; .*`},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			findings, err := sqllint.LintSource(t.Context(),
				sqllint.Source{Name: "probe.sql", SQL: "CREATE TABLE users (email TEXT NOT NULL);"},
				sqllint.Options{SQL: builtintest.Runtime().SQL(), Bodies: builtintest.Runtime(), Dialect: platform.Postgres, DisabledRules: []string{row.disabled}},
			)

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(findings, qt.IsNil)
		})
	}
}

// TestDisableStillReachesAnOpinionRule is the control the refusal needs.
//
// Without it, refusing every selector would pass: a --disable that never works
// is the flag this file was written to prevent.
func TestDisableStillReachesAnOpinionRule(t *testing.T) {
	c := qt.New(t)

	c.Assert(reportedCodes(t, "CREATE TABLE users (email TEXT NOT NULL);", []string{"design/no-primary-key"}),
		qt.DeepEquals, make([]string, 0))
}

// TestDisableRefusesAnUnknownName refuses a selector written as a Ptah name or
// category that names no rule this linter reports. The first row begins a
// real name, which a prefix match would accept.
func TestDisableRefusesAnUnknownName(t *testing.T) {
	rows := []struct {
		name     string
		disabled string
		wantErr  string
	}{
		{
			name:     "the beginning of a name",
			disabled: "design/no-primary",
			wantErr: `--disable "design/no-primary" is not the name of a SQL lint rule; ` +
				`a name is a category and a rule joined by a slash, such as locking/add-foreign-key`,
		},
		{
			name:     "a name only migration lint has",
			disabled: "locking/set-not-null",
			wantErr:  `--disable "locking/set-not-null" is not the name of a SQL lint rule; .*`,
		},
		{
			name:     "a category that is not declared",
			disabled: "desig/",
			wantErr:  `--disable "desig/" is not a rule category; the categories are capability, correctness, .*`,
		},
		{
			name:     "a category this linter has no rule in",
			disabled: "online",
			wantErr:  `--disable "online" is a category that holds no SQL lint rule`,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			findings, err := sqllint.LintSource(t.Context(),
				sqllint.Source{Name: "probe.sql", SQL: "CREATE TABLE users (email TEXT NOT NULL);"},
				sqllint.Options{SQL: builtintest.Runtime().SQL(), Bodies: builtintest.Runtime(), Dialect: platform.Postgres, DisabledRules: []string{row.disabled}},
			)

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(findings, qt.IsNil)
		})
	}
}

// TestFindingsCarryTheRuleName reads the Ptah name as a finding's rule.
func TestFindingsCarryTheRuleName(t *testing.T) {
	c := qt.New(t)

	findings, err := sqllint.LintSource(c.Context(),
		sqllint.Source{Name: "probe.sql", SQL: "CREATE TABLE users (email TEXT NOT NULL);"},
		sqllint.Options{SQL: builtintest.Runtime().SQL(), Bodies: builtintest.Runtime(), Dialect: platform.Postgres},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, "design/no-primary-key")
}
