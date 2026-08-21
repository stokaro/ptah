package lintcatalog_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/lintcatalog"
	"go.5x5.cz/ptah/migration/lint"
)

// The alias table lives in migration/lint, because internal/lintcatalog imports
// that package and the reverse would be a cycle. Two statements of the same
// mapping are two statements that drift, so this holds them together
// (stokaro/ptah#1631).

// TestAtlasAliasesMatchTheCheckCatalog requires every alias to name exactly the
// Ptah rules the Atlas check row names.
func TestAtlasAliasesMatchTheCheckCatalog(t *testing.T) {
	c := qt.New(t)
	byCode := make(map[string]lintcatalog.AtlasCheck)
	for _, check := range lintcatalog.AtlasChecks() {
		byCode[check.Code] = check
	}

	for code, rules := range lint.AtlasCodeAliases() {
		check, found := byCode[code]
		c.Assert(found, qt.IsTrue, qt.Commentf("alias %s names no documented Atlas check", code))
		c.Assert(rules, qt.DeepEquals, check.PtahRules,
			qt.Commentf("alias %s must name the rules its catalog row names", code))
	}
}

// TestEveryMappedCheckHasAnAlias is the other direction, and the one that
// matters when a new mapped check is added: a row whose Ptah rules are spelled
// differently from its own code needs an alias, or the code it documents is
// refused as unknown in a config.
//
// The exception is a code Ptah also uses for a rule of its own. This test
// found six of those, which is why they are excluded by name and with a reason
// rather than by being left out.
func TestEveryMappedCheckHasAnAlias(t *testing.T) {
	c := qt.New(t)
	aliases := lint.AtlasCodeAliases()

	for _, check := range checksNeedingAnAlias() {
		_, found := aliases[check.Code]
		c.Assert(found, qt.IsTrue,
			qt.Commentf("Atlas %s reports under %v, so a config naming %s needs an alias",
				check.Code, check.PtahRules, check.Code))
	}
}

// checksNeedingAnAlias lists the Atlas checks whose Ptah rules are spelled
// differently from their own code and that Ptah does not also use as a rule
// name of its own. The excluded ones are in
// [lint.AtlasCodesPtahAlsoUses], each with a reason.
func checksNeedingAnAlias() []lintcatalog.AtlasCheck {
	collisions := lint.AtlasCodesPtahAlsoUses()
	var needing []lintcatalog.AtlasCheck
	for _, check := range lintcatalog.AtlasChecks() {
		switch {
		case len(check.PtahRules) == 0:
		case len(check.PtahRules) == 1 && check.PtahRules[0] == check.Code:
			// The code IS the Ptah rule, so there is nothing to alias.
		default:
			if _, collides := collisions[check.Code]; !collides {
				needing = append(needing, check)
			}
		}
	}
	return needing
}

// TestExcludedAliasesReallyCollide is the control for that exclusion. An entry
// there must name a code Ptah registers as a rule -- otherwise it is not a
// collision, it is an alias somebody skipped.
func TestExcludedAliasesReallyCollide(t *testing.T) {
	c := qt.New(t)
	registered := make(map[string]struct{})
	for _, rule := range lint.Rules() {
		registered[rule.Code] = struct{}{}
	}

	for code := range lint.AtlasCodesPtahAlsoUses() {
		_, found := registered[code]
		c.Assert(found, qt.IsTrue,
			qt.Commentf("%s is excluded as colliding, but Ptah registers no rule by that name", code))
	}
}
