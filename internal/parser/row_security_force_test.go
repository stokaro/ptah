package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// TestParse_ForceRowLevelSecurity_HappyPath reads FORCE into a node of its own.
// A statement that only forces must not read as one that enables: a migration
// that forces an already enabled table enables nothing.
func TestParse_ForceRowLevelSecurity_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantTable string
	}{
		{name: "bare", sql: `ALTER TABLE sites FORCE ROW LEVEL SECURITY;`, wantTable: "sites"},
		{name: "only and qualified", sql: `ALTER TABLE ONLY "public"."sites" FORCE ROW LEVEL SECURITY;`, wantTable: `"public"."sites"`},
		{name: "lower case", sql: `alter table sites force row level security;`, wantTable: "sites"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.Postgres)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			force, isForce := statements.Statements[0].(*ast.AlterTableForceRLSNode)
			c.Assert(isForce, qt.IsTrue)
			c.Assert(force.Table, qt.Equals, test.wantTable)
			c.Assert(force.NoForce, qt.IsFalse)
		})
	}
}

// TestParse_ForceRowLevelSecurity_FailurePath refuses the forms that take a
// protection away, and keeps NO in front of anything but FORCE refused as it
// was.
func TestParse_ForceRowLevelSecurity_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "no force",
			sql:     `ALTER TABLE sites NO FORCE ROW LEVEL SECURITY;`,
			wantErr: `unsupported ALTER operation: NO FORCE ROW LEVEL SECURITY at position \d+`,
		},
		{
			name:    "no followed by something else",
			sql:     `ALTER TABLE sites NO INHERIT parent;`,
			wantErr: `unsupported ALTER operation: NO at position \d+`,
		},
		{
			name:    "force followed by something else",
			sql:     `ALTER TABLE sites FORCE;`,
			wantErr: `unsupported ALTER operation: FORCE at position \d+`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.Postgres)).Parse()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestParse_PolicyAs_HappyPath reads the AS clause into the policy node.
func TestParse_PolicyAs_HappyPath(t *testing.T) {
	tests := []struct {
		name            string
		sql             string
		wantRestrictive bool
	}{
		{name: "restrictive", sql: `CREATE POLICY p ON t AS RESTRICTIVE USING (true);`, wantRestrictive: true},
		{name: "permissive", sql: `CREATE POLICY p ON t AS PERMISSIVE USING (true);`, wantRestrictive: false},
		{name: "no clause", sql: `CREATE POLICY p ON t USING (true);`, wantRestrictive: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.Postgres)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			policy, isPolicy := statements.Statements[0].(*ast.CreatePolicyNode)
			c.Assert(isPolicy, qt.IsTrue)
			c.Assert(policy.Restrictive, qt.Equals, test.wantRestrictive)
		})
	}
}

// TestParse_PolicyAs_FailurePath refuses a kind other than the two, rather
// than reading it as either.
func TestParse_PolicyAs_FailurePath(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser(`CREATE POLICY p ON t AS SOMETIMES USING (true);`,
		parser.WithDialect(platform.Postgres)).Parse()

	c.Assert(err, qt.ErrorMatches, `.*expected PERMISSIVE or RESTRICTIVE after AS, got SOMETIMES at position \d+`)
	c.Assert(statements, qt.IsNil)
}
