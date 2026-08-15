package atlasschema_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/migration/lint"
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
			name:    "a lint block naming the destructive family",
			configs: map[string]lint.RuleConfig{"DS": {}},
			want:    []string{"DS101"},
		},
		{
			name:    "a lint block naming another family",
			configs: map[string]lint.RuleConfig{"MF": {}},
			want:    []string{},
		},
		{
			name:    "no lint block at all",
			configs: nil,
			want:    []string{},
		},
		{
			name:    "a lint block whose only selector is blank",
			configs: map[string]lint.RuleConfig{"   ": {}},
			want:    []string{},
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			findings, err := atlasschema.LintPlan(destructivePlan, atlasschema.PlanLintOptions{
				Dialect:     "postgres",
				RuleConfigs: row.configs,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(codesOf(findings), qt.DeepEquals, row.want)
		})
	}
}
