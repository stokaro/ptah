package lint

// White-box testing required: the alias expansion is unexported and sits
// between the policy and the rule engine, where no exported call reports which
// rules a selector silenced for a given dialect.

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
)

// aliasSuppressionRow is one (dialect, selector) pair and whether the generic
// Ptah rule it aliases must end up suppressed.
type aliasSuppressionRow struct {
	name     string
	dialect  string
	selector string
	rule     string
	want     bool
}

// scopedAliases is an alias table with one entry per shape the scoping has
// to answer for. The built-in table holds no engine-specific alias any more
// -- every such code became a Ptah rule of its own name -- so the hazard is
// pinned against a table that has one, through the same expansion the
// built-in table goes through.
var scopedAliases = map[string]atlasAlias{
	"PG9XX": {dialects: []string{"postgres"}, rules: []string{"DS103"}},
	"MY9XX": {dialects: mysqlFamily, rules: []string{"CD103"}},
	"XX9XX": {rules: []string{"BC101"}},
}

func TestExpandAtlasCodeSelectorsScopesSuppressionToTheAliasDialect(t *testing.T) {
	rows := []aliasSuppressionRow{{
		name:     "postgres code silences the generic rule on postgres",
		dialect:  "postgres",
		selector: "PG9XX",
		rule:     "DS103",
		want:     true,
	}, {
		// The hazard this test exists for: a shared policy carrying a
		// PostgreSQL entry must not weaken a MySQL run.
		name:     "postgres code leaves the generic rule alone on mysql",
		dialect:  "mysql",
		selector: "PG9XX",
		rule:     "DS103",
		want:     false,
	}, {
		name:     "mysql code silences the generic rule on mysql",
		dialect:  "mysql",
		selector: "MY9XX",
		rule:     "CD103",
		want:     true,
	}, {
		name:     "mysql code also covers mariadb",
		dialect:  "mariadb",
		selector: "MY9XX",
		rule:     "CD103",
		want:     true,
	}, {
		name:     "mysql code leaves the generic rule alone on postgres",
		dialect:  "postgres",
		selector: "MY9XX",
		rule:     "CD103",
		want:     false,
	}, {
		// An alias with no engine of its own keeps applying everywhere.
		name:     "engine-neutral alias applies on any dialect",
		dialect:  "mysql",
		selector: "XX9XX",
		rule:     "BC101",
		want:     true,
	}, {
		// Matches the rule engine's own convention for an unset dialect.
		name:     "no configured dialect expands every alias",
		dialect:  "",
		selector: "PG9XX",
		rule:     "DS103",
		want:     true,
	}}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			expanded := expandSelectorsWithAliases(scopedAliases, []string{row.selector}, row.dialect)
			c.Assert(slices.Contains(expanded, row.rule), qt.Equals, row.want)
		})
	}
}

// TestRuleDisabledGoesThroughTheBuiltInAliases is the control that the
// built-in table reaches the rule engine the same way: the one alias left
// with rules of a different spelling disables them on every dialect.
func TestRuleDisabledGoesThroughTheBuiltInAliases(t *testing.T) {
	c := qt.New(t)

	registered := registeredCodes(Rules())
	c.Assert(ruleDisabled("BC101", []string{"BC102"}, "mysql", registered), qt.IsTrue)
	c.Assert(ruleDisabled("LT101", []string{"MF104"}, "sqlite", registered), qt.IsTrue)
	c.Assert(ruleDisabled("DS103", []string{"PG301"}, "postgres", registered), qt.IsFalse)
}

func TestExpandAtlasCodeSelectorsKeepsTheOriginalSelector(t *testing.T) {
	c := qt.New(t)

	// A selector is a prefix, so the entry the operator wrote has to survive
	// expansion or `PG3` would stop selecting what it selected before.
	c.Assert(expandAtlasCodeSelectorsForDialect([]string{"PG301"}, "mysql"), qt.DeepEquals, []string{"PG301"})
	c.Assert(expandAtlasCodeSelectorsForDialect([]string{"PG3"}, "postgres"), qt.DeepEquals, []string{"PG3"})
}

func TestExpandAtlasCodeSelectorsValidatesAcrossEveryDialect(t *testing.T) {
	c := qt.New(t)

	// Validation is deliberately NOT scoped: a policy shared across engines
	// names PG301 legitimately even while linting MySQL, and refusing it as
	// unknown is the failure stokaro/ptah#1631 fixed.
	c.Assert(selectorMatchesRule("BC102", Rules()), qt.IsTrue)
	c.Assert(selectorMatchesRule("MF104", Rules()), qt.IsTrue)
}
