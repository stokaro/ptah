package atlasschema_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
)

// This file measures how much of the rule registry `ptah-compat schema apply`
// runs, because the reference page tells an operator so.
//
// The answer is "only what the atlas.hcl lint block names, and nothing at all
// without one". An operator who read the rule tables as the set of checks
// standing between a declarative apply and a live database would be reading a
// longer list than this path consults.

// destructivePlan is a planned apply that drops a table: the statement every
// destructive rule in the registry is written to catch. What varies between the
// rows below is the policy, never the SQL, so a row that reports nothing
// reports nothing because of its policy.
var destructivePlan = []string{"DROP TABLE users;"}

// codesOf reduces findings to the sorted set of identifiers they report under.
func codesOf(findings []lint.Finding) []string {
	codes := make([]string, 0, len(findings))
	for _, finding := range findings {
		codes = append(codes, finding.Rule)
	}
	slices.Sort(codes)
	return slices.Compact(codes)
}

// TestLintPlan_RunsOnlyTheRulesThePolicyNames walks the four policy shapes an
// atlas.hcl can present. The first row is the one that proves the plan is
// lintable at all: without it, the three empty rows would agree just as well
// with a build that had stopped linting entirely.
func TestLintPlan_RunsOnlyTheRulesThePolicyNames(t *testing.T) {
	rows := []struct {
		name    string
		configs map[string]lint.RuleConfig
		want    []string
	}{
		{
			name:    "a lint block naming the destructive analyzer",
			configs: map[string]lint.RuleConfig{"destructive": {}},
			want:    []string{"destructive/drop-table"},
		},
		{
			name:    "a lint block naming another analyzer",
			configs: map[string]lint.RuleConfig{"concurrent_index": {}},
			want:    make([]string, 0),
		},
		{
			name:    "no lint block at all",
			configs: nil,
			want:    make([]string, 0),
		},
		{
			name:    "a lint block whose only selector is blank",
			configs: map[string]lint.RuleConfig{"   ": {}},
			want:    make([]string, 0),
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			findings, err := atlasschema.LintPlan(c.Context(), builtintest.Runtime(), destructivePlan, atlasschema.PlanLintOptions{
				Dialect:     "postgres",
				RuleConfigs: row.configs,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(codesOf(findings), qt.DeepEquals, row.want)
		})
	}
}

// TestFormatPlanLintFindings_NamesTheRuleByTheDialectsCode holds the refusal
// to the code each engine reports: rewrite/alter-column-type is Atlas PG301 on
// PostgreSQL and MY130 on MySQL, so a lookup that ignores the dialect fails one
// row whichever engine it falls back to. A rule Atlas has no check for keeps
// its name.
func TestFormatPlanLintFindings_NamesTheRuleByTheDialectsCode(t *testing.T) {
	tests := []struct {
		name    string
		rule    string
		dialect string
		want    string
	}{
		{name: "postgres", rule: "rewrite/alter-column-type", dialect: "postgres", want: "\n  - PG301 error: m"},
		{name: "mysql", rule: "rewrite/alter-column-type", dialect: "mysql", want: "\n  - MY130 error: m"},
		{name: "a rule with no Atlas check", rule: "destructive/truncate-table", dialect: "postgres", want: "\n  - destructive/truncate-table error: m"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := atlasschema.FormatPlanLintFindings([]lint.Finding{{Rule: test.rule, Severity: lint.SeverityError, Message: "m"}}, test.dialect)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}
