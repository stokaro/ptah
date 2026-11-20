package atlasschema

// White-box testing required: the enabled/disabled split the schema-apply lint
// pass builds is internal, and an aliased rule that never runs is invisible
// from outside — the pass simply reports no findings.

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/internal/atlascodes"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
)

// aliasPolicyRow is one schema-apply policy entry, as the atlas.hcl `lint`
// block spells it, and whether the Ptah rule it stands for must be left
// running.
type aliasPolicyRow struct {
	name        string
	policyKey   string
	aliasedRule string
	wantRuns    bool
}

// aliasedRuleRuns reports whether the pass would run rule, which is the whole
// question: a rule in the disabled set produces no findings and the plan
// applies clean.
func aliasedRuleRuns(policyKey, rule string) bool {
	configs := atlascodes.RuleConfigKeys(map[string]lint.RuleConfig{policyKey: {}})
	enabled := lintPlanEnabledSelectors(configs)
	return !slices.Contains(lintPlanDisabledNames(must.Must(lint.Rules(builtintest.Runtime())), enabled), rule)
}

func TestLintPlanEnabledSelectorsKeepsAnAliasedRuleRunning(t *testing.T) {
	rows := []aliasPolicyRow{{
		// The bug: the raw selector matched no registered rule, so the rule
		// the policy asked for was placed in the disabled list and the unsafe
		// plan applied clean.
		name:        "atlas spelling enables the ptah rule it stands for",
		policyKey:   "BC102",
		aliasedRule: "rollout/rename",
		wantRuns:    true,
	}, {
		name:        "an atlas code that is its own ptah rule's check",
		policyKey:   "MY133",
		aliasedRule: "rewrite/drop-primary-key",
		wantRuns:    true,
	}, {
		// An Atlas code does not expand to the generic rule it would
		// otherwise be mistaken for.
		name:        "postgres spelling does not enable the generic rule",
		policyKey:   "PG301",
		aliasedRule: "data-dependent/alter-column-type",
		wantRuns:    false,
	}, {
		name:        "a ptah name keeps working unchanged",
		policyKey:   "data-dependent/alter-column-type",
		aliasedRule: "data-dependent/alter-column-type",
		wantRuns:    true,
	}, {
		// A name selects itself alone: the missing-down rule is not enabled
		// by a policy naming the unique-index check.
		name:        "a name does not reach its neighbor",
		policyKey:   "data-dependent/add-unique-index",
		aliasedRule: "file-form/missing-down",
		wantRuns:    false,
	}, {
		name:        "a category reaches every rule in it",
		policyKey:   "file-form",
		aliasedRule: "file-form/missing-down",
		wantRuns:    true,
	}, {
		name:        "an analyzer reaches the rules it covers",
		policyKey:   "incompatible",
		aliasedRule: "rollout/drop-column",
		wantRuns:    true,
	}}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(aliasedRuleRuns(row.policyKey, row.aliasedRule), qt.Equals, row.wantRuns)
		})
	}
}

func TestLintPlanEnabledSelectorsLeavesUnrelatedRulesDisabled(t *testing.T) {
	c := qt.New(t)

	// The pass runs exactly the policy's rules; translating an Atlas
	// spelling widens the enabled set by the rules it names only, not open
	// the whole catalog.
	configs := atlascodes.RuleConfigKeys(map[string]lint.RuleConfig{"BC102": {}})
	enabled := lintPlanEnabledSelectors(configs)
	disabled := lintPlanDisabledNames(must.Must(lint.Rules(builtintest.Runtime())), enabled)

	c.Assert(enabled, qt.DeepEquals, []string{"rollout/rename"})
	c.Assert(disabled, qt.Not(qt.Contains), "rollout/rename")
	c.Assert(disabled, qt.Contains, "rollout/drop-table")
}
