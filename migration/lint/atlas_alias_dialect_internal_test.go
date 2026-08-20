package lint

// White-box testing required: the alias expansion is unexported and sits
// between the policy and the rule engine, where no exported call reports which
// rules a selector silenced for a given dialect.

import (
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

func TestExpandAtlasCodeSelectorsScopesSuppressionToTheAliasDialect(t *testing.T) {
	rows := []aliasSuppressionRow{{
		name:     "postgres code silences the generic rule on postgres",
		dialect:  "postgres",
		selector: "PG301",
		rule:     "DS103",
		want:     true,
	}, {
		// The hazard this test exists for: a shared policy carrying a
		// PostgreSQL entry must not weaken a MySQL run.
		name:     "postgres code leaves the generic rule alone on mysql",
		dialect:  "mysql",
		selector: "PG301",
		rule:     "DS103",
		want:     false,
	}, {
		name:     "mysql code silences the generic rule on mysql",
		dialect:  "mysql",
		selector: "MY110",
		rule:     "DS103",
		want:     true,
	}, {
		name:     "mysql code also covers mariadb",
		dialect:  "mariadb",
		selector: "MY133",
		rule:     "CD103",
		want:     true,
	}, {
		name:     "mysql code leaves the generic rule alone on postgres",
		dialect:  "postgres",
		selector: "MY110",
		rule:     "DS103",
		want:     false,
	}, {
		// An alias with no engine of its own keeps applying everywhere.
		name:     "engine-neutral alias applies on any dialect",
		dialect:  "mysql",
		selector: "BC102",
		rule:     "BC101",
		want:     true,
	}, {
		// Matches the rule engine's own convention for an unset dialect.
		name:     "no configured dialect expands every alias",
		dialect:  "",
		selector: "PG301",
		rule:     "DS103",
		want:     true,
	}}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ruleDisabled(row.rule, []string{row.selector}, row.dialect), qt.Equals, row.want)
		})
	}
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
	c.Assert(selectorMatchesRule("PG301", Rules()), qt.IsTrue)
	c.Assert(selectorMatchesRule("MY110", Rules()), qt.IsTrue)
}
