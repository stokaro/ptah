package atlasschema

// White-box testing required: the enabled/disabled split the schema-apply lint
// pass builds is internal, and an aliased rule that never runs is invisible
// from outside — the pass simply reports no findings.

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/lint"
)

// aliasPolicyRow is one schema-apply policy entry plus the dialect it is linted
// for, and whether the Ptah rule it aliases must be left running.
type aliasPolicyRow struct {
	name        string
	policyCode  string
	dialect     string
	aliasedRule string
	wantRuns    bool
}

// aliasedRuleRuns reports whether the pass would run rule, which is the whole
// question: a rule in the disabled set produces no findings and the plan
// applies clean.
func aliasedRuleRuns(policyCode, dialect, rule string) bool {
	enabled := lintPlanEnabledCodes(map[string]lint.RuleConfig{policyCode: {}}, dialect)
	return !slices.Contains(lintPlanDisabledCodes(enabled), rule)
}

func TestLintPlanEnabledCodesKeepsAnAliasedRuleRunning(t *testing.T) {
	rows := []aliasPolicyRow{{
		// The bug: the raw selector matched no registered code, so the rule
		// the policy asked for was placed in the disabled list and the unsafe
		// plan applied clean.
		name:        "atlas spelling enables the ptah rule it stands for",
		policyCode:  "PG301",
		dialect:     "postgres",
		aliasedRule: "DS103",
		wantRuns:    true,
	}, {
		name:        "mysql spelling enables its ptah rule on mysql",
		policyCode:  "MY133",
		dialect:     "mysql",
		aliasedRule: "CD103",
		wantRuns:    true,
	}, {
		// A PostgreSQL policy entry must not reach into a MySQL run.
		name:        "postgres spelling does not enable the generic rule on mysql",
		policyCode:  "PG301",
		dialect:     "mysql",
		aliasedRule: "DS103",
		wantRuns:    false,
	}, {
		name:        "ptah spelling keeps working unchanged",
		policyCode:  "DS103",
		dialect:     "postgres",
		aliasedRule: "DS103",
		wantRuns:    true,
	}}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(aliasedRuleRuns(row.policyCode, row.dialect, row.aliasedRule),
				qt.Equals, row.wantRuns)
		})
	}
}

func TestLintPlanEnabledCodesLeavesUnrelatedRulesDisabled(t *testing.T) {
	c := qt.New(t)

	// The pass runs exactly the policy's rules; expanding an alias must widen
	// the enabled set by the aliased rule only, not open the whole catalog.
	enabled := lintPlanEnabledCodes(map[string]lint.RuleConfig{"PG301": {}}, "postgres")
	disabled := lintPlanDisabledCodes(enabled)

	c.Assert(enabled, qt.DeepEquals, []string{"PG301", "DS103"})
	c.Assert(disabled, qt.Contains, "BC101")
	c.Assert(disabled, qt.Not(qt.Contains), "DS103")
}
