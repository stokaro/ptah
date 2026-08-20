package lint_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/lint"
)

// A code is the unit of suppression, of severity policy, and of "we accept this
// risk" in a review. Five Atlas codes report here under a broader Ptah one, so
// a user who wrote the code the Atlas documentation tells them to write was
// refused it as unknown (stokaro/ptah#1631).

func TestAtlasCodeAliases_CoverEveryMappedCodeThatCanBeAliased(t *testing.T) {
	c := qt.New(t)

	aliases := lint.AtlasCodeAliases()

	// Fourteen Atlas codes report under a differently spelled Ptah rule; six of
	// them are codes Ptah also uses for a rule of its own and are excluded.
	c.Assert(aliases, qt.HasLen, 8)
	c.Assert(aliases["PG301"], qt.DeepEquals, []string{"DS103"})
	c.Assert(aliases["PG304"], qt.DeepEquals, []string{"PG104"})
	c.Assert(aliases["MY136"], qt.DeepEquals, []string{"MY101"})
	c.Assert(aliases["MY130"], qt.DeepEquals, []string{"MY101", "DS103"})
	c.Assert(aliases["MY133"], qt.DeepEquals, []string{"CD103"})
}

// TestAtlasCodeFor_AnswersEveryCodeARuleStandsFor holds the reverse direction,
// which a report needs: one Ptah rule can stand in for more than one Atlas
// code, so the answer is a list rather than a single name.
func TestAtlasCodeFor_AnswersEveryCodeARuleStandsFor(t *testing.T) {
	c := qt.New(t)

	c.Assert(lint.AtlasCodeFor("DS103"), qt.DeepEquals, []string{"MY110", "MY130", "PG301"})
	c.Assert(lint.AtlasCodeFor("MY101"), qt.DeepEquals, []string{"MY110", "MY130", "MY136"})
	c.Assert(lint.AtlasCodeFor("PG104"), qt.DeepEquals, []string{"PG304"})
	c.Assert(lint.AtlasCodeFor("PG302"), qt.HasLen, 0)
}

// lintFS runs the linter over one migration file.
func lintFS(c *qt.C, sql string, opts lint.Options) []lint.Finding {
	c.Helper()
	opts.Dialect = "postgres"
	findings, err := lint.LintFS(fstest.MapFS{
		"0000000001_change.up.sql":   &fstest.MapFile{Data: []byte(sql)},
		"0000000001_change.down.sql": &fstest.MapFile{Data: []byte("SELECT 1;\n")},
	}, opts)
	c.Assert(err, qt.IsNil)
	return findings
}

// TestAtlasCodeSelectorDisablesTheRuleThatReportsIt is the behavior the issue
// asks for: the Atlas code is accepted where a Ptah code is.
func TestAtlasCodeSelectorDisablesTheRuleThatReportsIt(t *testing.T) {
	c := qt.New(t)
	sql := "ALTER TABLE t ALTER COLUMN c TYPE bigint;\n"

	withRule := lintFS(c, sql, lint.Options{})
	withoutRule := lintFS(c, sql, lint.Options{Disabled: []string{"PG301"}})

	c.Assert(lintCodes(withRule), qt.Contains, "DS103")
	c.Assert(lintCodes(withoutRule), qt.Not(qt.Contains), "DS103")
}

// TestAtlasCodeSelectorLeavesOtherRulesAlone is the control: an alias must
// silence the rule it maps to and nothing else.
func TestAtlasCodeSelectorLeavesOtherRulesAlone(t *testing.T) {
	c := qt.New(t)
	sql := "ALTER TABLE t ALTER COLUMN c TYPE bigint;\nDROP TABLE other;\n"

	findings := lintFS(c, sql, lint.Options{Disabled: []string{"PG301"}})

	c.Assert(lintCodes(findings), qt.Not(qt.Contains), "DS103")
	c.Assert(len(lintCodes(findings)) > 0, qt.IsTrue,
		qt.Commentf("the DROP must still be reported: %v", lintCodes(findings)))
}

// TestAtlasCodeSeverityOverrideReachesTheRule holds the other half of what a
// code is for: policy, not only suppression.
func TestAtlasCodeSeverityOverrideReachesTheRule(t *testing.T) {
	c := qt.New(t)
	sql := "ALTER TABLE t ALTER COLUMN c TYPE bigint;\n"

	findings := lintFS(c, sql, lint.Options{
		RuleConfigs: map[string]lint.RuleConfig{"PG301": {Severity: lint.SeverityWarning}},
	})

	c.Assert(severityOf(c, findings, "DS103"), qt.Equals, string(lint.SeverityWarning))
}

// severityOf returns the severity one rule was reported at, failing when it was
// not reported at all.
func severityOf(c *qt.C, findings []lint.Finding, rule string) string {
	c.Helper()
	for _, finding := range findings {
		if finding.Rule == rule {
			return string(finding.Severity)
		}
	}
	c.Fatalf("%s was not reported: %v", rule, lintCodes(findings))
	return ""
}

// lintCodes lists the codes a run reported.
func lintCodes(findings []lint.Finding) []string {
	codes := make([]string, 0, len(findings))
	for _, finding := range findings {
		codes = append(codes, finding.Rule)
	}
	return codes
}

// TestAtlasCodesPtahAlsoUses_AreNotAliased is the boundary that keeps this from
// changing what an existing config does. Atlas DS103 reports under Ptah DS102,
// and Ptah has its own DS103 for a different hazard: aliasing would make
// `--disable DS103` silence two rules where the operator asked for one.
func TestAtlasCodesPtahAlsoUses_AreNotAliased(t *testing.T) {
	c := qt.New(t)
	aliases := lint.AtlasCodeAliases()

	for code, reason := range lint.AtlasCodesPtahAlsoUses() {
		_, aliased := aliases[code]
		c.Assert(aliased, qt.IsFalse, qt.Commentf("%s must not be aliased: %s", code, reason))
		c.Assert(reason, qt.Not(qt.Equals), "")
	}
}

// TestCollidingCodeStillSelectsOnlyTheRuleItNames is the behavioral half of the
// same boundary, asserted on the linter rather than on the table.
func TestCollidingCodeStillSelectsOnlyTheRuleItNames(t *testing.T) {
	c := qt.New(t)
	sql := "ALTER TABLE t ALTER COLUMN c TYPE bigint;\n"

	findings := lintFS(c, sql, lint.Options{Disabled: []string{"DS103"}})

	c.Assert(lintCodes(findings), qt.Not(qt.Contains), "DS103")
	c.Assert(lintCodes(findings), qt.Not(qt.Contains), "DS102")
}
