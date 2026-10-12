package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/parser"
	"ptah.run/migration/safety"
)

// extensionDropReason is the reason a dropped extension is destructive.
const extensionDropReason = "DROP EXTENSION removes database objects owned by the extension"

// TestAssessSQL_AnExtensionDropIsDestructive pins the SQL entry, which schema
// apply's plan uses: DROP EXTENSION is destructive by its words, guarded,
// cascading or neither, on its target and with none.
func TestAssessSQL_AnExtensionDropIsDestructive(t *testing.T) {
	tests := []struct {
		target    string
		statement string
	}{
		{target: platform.Postgres, statement: "DROP EXTENSION pg_trgm"},
		{target: platform.Postgres, statement: "DROP EXTENSION IF EXISTS pg_trgm CASCADE"},
		{target: "", statement: "drop extension pg_trgm cascade"},
	}
	for _, test := range tests {
		t.Run(test.target+"/"+test.statement, func(t *testing.T) {
			c := qt.New(t)

			got := assessText(c, test.target, test.statement)

			c.Assert(got.Severity, qt.Equals, safety.Destructive)
			c.Assert(got.Reason, qt.Equals, extensionDropReason)
		})
	}
}

// TestAssess_AnExtensionChangeTheOwnerReads pins the statement entry over
// what the extension owner reads from a migration's SQL: its operation
// decides, and a drop with CASCADE says it removes the dependents too.
func TestAssess_AnExtensionChangeTheOwnerReads(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		severity safety.Severity
		reason   string
	}{
		{name: "a drop", sql: "DROP EXTENSION pg_trgm;", severity: safety.Destructive, reason: extensionDropReason},
		{name: "a drop with its dependents", sql: "DROP EXTENSION IF EXISTS pg_trgm CASCADE;", severity: safety.Destructive,
			reason: "DROP EXTENSION ... CASCADE removes database objects owned by the extension and every object that depends on them"},
		{name: "an update", sql: "ALTER EXTENSION pg_trgm UPDATE TO '1.6';", severity: safety.Warning,
			reason: "changes the version or the schema of an extension and of the objects it owns"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.Postgres), parser.WithSQL(builtintest.Runtime().SQL())).Parse()
			c.Assert(err, qt.IsNil)

			assessments := safety.Assess(statements.Statements)

			c.Assert(assessments, qt.HasLen, 1)
			c.Assert(assessments[0].Severity, qt.Equals, test.severity)
			c.Assert(assessments[0].Reason, qt.Equals, test.reason)
			c.Assert(assessments[0].Subject, qt.Equals, "pg_trgm")
		})
	}
}
