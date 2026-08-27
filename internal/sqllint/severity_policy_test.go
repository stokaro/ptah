package sqllint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/sqllint"
)

// TestSeverities_APolicyReplacesARuleSeverity pins that `ptah sql lint` honors
// the severity a project sets, which #1270 asks for as "Rule severity can be
// configured through the normal Ptah policy mechanism".
//
// Before this, the two halves existed and were not joined: the `.ptah-lint.yaml`
// reader with per-rule overrides, and a gate-checked enumeration of the SQL
// identifiers. `ptah sql lint` read no policy at all.
func TestSeverities_APolicyReplacesARuleSeverity(t *testing.T) {
	rows := []struct {
		name       string
		severities map[string]sqllint.Severity
		want       sqllint.Severity
	}{
		{
			name: "no policy leaves the default",
			want: sqllint.SeverityWarning,
		},
		{
			name:       "raised to error",
			severities: map[string]sqllint.Severity{"DDL001": sqllint.SeverityError},
			want:       sqllint.SeverityError,
		},
		{
			name:       "lowered to info",
			severities: map[string]sqllint.Severity{"DDL001": sqllint.SeverityInfo},
			want:       sqllint.SeverityInfo,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			findings, err := sqllint.LintSource(
				sqllint.Source{Name: "t.sql", SQL: "CREATE TABLE users (email TEXT NOT NULL);"},
				sqllint.Options{Dialect: platform.Postgres, Severities: row.severities},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(findings, qt.HasLen, 1)
			c.Assert(findings[0].Rule, qt.Equals, sqllint.RuleTableWithoutPrimaryKey)
			c.Assert(findings[0].Severity, qt.Equals, row.want)
		})
	}
}

// TestSeverities_APolicyThisLinterCannotHonorIsRefused pins the two refusals.
//
// A policy that reads as being in force while doing nothing, or the wrong
// thing, is the failure `--disable` had before it refused.
func TestSeverities_APolicyThisLinterCannotHonorIsRefused(t *testing.T) {
	rows := []struct {
		name       string
		severities map[string]sqllint.Severity
		wants      string
	}{
		{
			// An operator who wrote this believes a rule is configured.
			name:       "a code this linter does not report",
			severities: map[string]sqllint.Severity{"DS101": sqllint.SeverityWarning},
			wants:      "does not report",
		},
		{
			// Only error decides the exit code, so this would let a file the
			// parser could not read pass.
			name:       "a parse-path code lowered below error",
			severities: map[string]sqllint.Severity{"SQL001": sqllint.SeverityWarning},
			wants:      "could not be analyzed",
		},
		{
			name:       "the other parse-path code, lowered to info",
			severities: map[string]sqllint.Severity{"SQL002": sqllint.SeverityInfo},
			wants:      "could not be analyzed",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := sqllint.LintSource(
				sqllint.Source{Name: "t.sql", SQL: "CREATE TABLE users (email TEXT NOT NULL);"},
				sqllint.Options{Dialect: platform.Postgres, Severities: row.severities},
			)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, row.wants)
		})
	}
}

// TestSeverities_AParsePathCodeMayBeSetToWhatItAlreadyIs is the control the
// refusal above needs: refusing every entry for those codes would pass it, and
// a policy that names a rule at its own severity is not a mistake.
func TestSeverities_AParsePathCodeMayBeSetToWhatItAlreadyIs(t *testing.T) {
	c := qt.New(t)

	_, err := sqllint.LintSource(
		sqllint.Source{Name: "t.sql", SQL: "CREATE TABLE users (email TEXT NOT NULL);"},
		sqllint.Options{
			Dialect:    platform.Postgres,
			Severities: map[string]sqllint.Severity{"SQL001": sqllint.SeverityError},
		},
	)

	c.Assert(err, qt.IsNil)
}
