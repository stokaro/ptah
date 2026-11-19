package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
)

// Oracle checks every row against a disabled or unvalidated constraint and
// enforces it with ENABLE [VALIDATE] CONSTRAINT, which a plan writes to bring
// a constraint to its declared state, so a migration that holds it reads
// back. ENABLE alone validates, as Oracle reads it.
func TestParse_EnableConstraint_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "ENABLE VALIDATE", sql: "ALTER TABLE t1 ENABLE VALIDATE CONSTRAINT ck11;"},
		{name: "ENABLE alone", sql: "ALTER TABLE t1 ENABLE CONSTRAINT ck11;"},
		{name: "lower case", sql: "alter table t1 enable validate constraint ck11;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseAs("oracle", test.sql)

			c.Assert(err, qt.IsNil)
			alter, ok := statements.Statements[0].(*ast.AlterTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(alter.Operations, qt.DeepEquals, []ast.AlterOperation{&ast.ValidateConstraintOperation{ConstraintName: "ck11"}})
		})
	}
}

// ENABLE NOVALIDATE leaves the rows a table holds unchecked, a state no
// declaration states, and an engine without Oracle's statement refuses ENABLE
// as an ALTER operation.
func TestParse_EnableConstraint_FailurePath(t *testing.T) {
	tests := []struct {
		name, dialect, sql, wantErr string
	}{
		{
			name: "ENABLE NOVALIDATE", dialect: "oracle", sql: "ALTER TABLE t1 ENABLE NOVALIDATE CONSTRAINT ck11;",
			wantErr: `line 1, column 23: ENABLE NOVALIDATE CONSTRAINT leaves the rows the table holds unchecked, a state no ` +
				`declaration states; write ENABLE VALIDATE CONSTRAINT`,
		},
		{
			name: "ENABLE without CONSTRAINT", dialect: "oracle", sql: "ALTER TABLE t1 ENABLE ROW MOVEMENT;",
			wantErr: `line 1, column 23: expected CONSTRAINT after ENABLE: expected 'CONSTRAINT', got 'ROW'`,
		},
		{
			name: "PostgreSQL", dialect: "postgres", sql: "ALTER TABLE t1 ENABLE VALIDATE CONSTRAINT ck11;",
			wantErr: `line 1, column 16: unsupported ALTER operation: ENABLE`,
		},
		{
			name: "MySQL", dialect: "mysql", sql: "ALTER TABLE t1 ENABLE VALIDATE CONSTRAINT ck11;",
			wantErr: `line 1, column 16: unsupported ALTER operation: ENABLE`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseAs(test.dialect, test.sql)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
