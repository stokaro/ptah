package lint_test

import (
	"fmt"
	"slices"
	"sync/atomic"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/lint"
)

var externalRuleSequence atomic.Uint64

func nextExternalRuleCode() string {
	return fmt.Sprintf("ZZ%06d", externalRuleSequence.Add(1))
}

func TestRegisterCustomRuleFromExternalPackage(t *testing.T) {
	c := qt.New(t)
	ruleCode := nextExternalRuleCode()

	err := lint.Register(lint.Rule{
		Code:     ruleCode,
		Title:    "external analyzer sentinel",
		Severity: lint.SeverityWarning,
		CheckStatement: func(stmt *lint.Statement) (bool, string) {
			return stmt.Canonical == "SELECT 424242", "external analyzer used Ptah's prepared statement"
		},
	})
	c.Assert(err, qt.IsNil)

	findings, err := lint.LintFS(fixture(map[string]string{
		"0000000001_custom.up.sql":   "SELECT 424242;\n",
		"0000000001_custom.down.sql": "-- restore\n",
	}), lint.Options{})

	c.Assert(err, qt.IsNil)
	registeredIndex := slices.IndexFunc(findings, func(finding lint.Finding) bool {
		return finding.Rule == ruleCode
	})
	c.Assert(registeredIndex, qt.Not(qt.Equals), -1)
	c.Assert(findings[registeredIndex].Message, qt.Contains, "Ptah's prepared statement")
}

func TestRegisterCustomRuleClonesMutableDialectState(t *testing.T) {
	c := qt.New(t)
	ruleCode := nextExternalRuleCode()
	dialects := []string{"postgres"}

	err := lint.Register(lint.Rule{
		Code:     ruleCode,
		Title:    "registry copy isolation",
		Severity: lint.SeverityWarning,
		Dialects: dialects,
		CheckStatement: func(*lint.Statement) (bool, string) {
			return false, ""
		},
	})
	c.Assert(err, qt.IsNil)
	dialects[0] = "mysql"

	rules := lint.Rules()
	registeredIndex := slices.IndexFunc(rules, func(rule lint.Rule) bool {
		return rule.Code == ruleCode
	})
	c.Assert(registeredIndex, qt.Not(qt.Equals), -1)
	c.Assert(rules[registeredIndex].Dialects, qt.DeepEquals, []string{"postgres"})
	rules[registeredIndex].Dialects[0] = "sqlite"

	freshRules := lint.Rules()
	freshIndex := slices.IndexFunc(freshRules, func(rule lint.Rule) bool {
		return rule.Code == ruleCode
	})
	c.Assert(freshIndex, qt.Not(qt.Equals), -1)
	c.Assert(freshRules[freshIndex].Dialects, qt.DeepEquals, []string{"postgres"})
}
