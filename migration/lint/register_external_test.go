package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/lint"
)

func TestRegisterCustomRuleFromExternalPackage(t *testing.T) {
	c := qt.New(t)

	err := lint.Register(lint.Rule{
		Code:     "ZZ999",
		Title:    "external analyzer sentinel",
		Severity: lint.SeverityWarning,
		CheckStatement: func(stmt *lint.Statement) (bool, string) {
			if stmt.Canonical == "SELECT 424242" {
				return true, "external analyzer used Ptah's prepared statement"
			}
			return false, ""
		},
	})
	c.Assert(err, qt.IsNil)

	findings, err := lint.LintFS(fixture(map[string]string{
		"0000000001_custom.up.sql":   "SELECT 424242;\n",
		"0000000001_custom.down.sql": "-- restore\n",
	}), lint.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, "ZZ999")
	c.Assert(findings[0].Message, qt.Contains, "Ptah's prepared statement")
}
