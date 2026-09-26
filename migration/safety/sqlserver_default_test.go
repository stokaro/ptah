package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/migration/safety"
)

// defaultChange is the ALTER TABLE that changes the default of users.status.
func defaultChange(operation *ast.AlterColumnOperation) ast.Node {
	return &ast.AlterTableNode{Name: "dbo.users", Operations: []ast.AlterOperation{operation}}
}

// On SQL Server a default is a constraint, so dropping one renders as a DROP
// CONSTRAINT whose name the statement reads from sys.default_constraints. Read
// by its words that is "removes an existing data protection"; it removes a
// default, and it is reported the way dropping a default is on every other
// engine. Replacing one drops the old default and adds the new one in the same
// statement, and the column keeps a default: that is reported as setting one
// is elsewhere (stokaro/ptah#3650).
func TestAssessRendered_SQLServerDefaultChange(t *testing.T) {
	tests := []struct {
		name         string
		operation    *ast.AlterColumnOperation
		wantSeverity safety.Severity
		wantReason   string
	}{
		{
			name:         "DROP DEFAULT",
			operation:    &ast.AlterColumnOperation{ColumnName: "status", Action: ast.AlterColumnDropDefault},
			wantSeverity: safety.Warning,
			wantReason:   "DROP DEFAULT can break writers that leave the column out",
		},
		{
			name: "SET DEFAULT",
			operation: &ast.AlterColumnOperation{
				ColumnName: "status", Action: ast.AlterColumnSetDefault, Default: &ast.DefaultValue{Value: "active", ValueSet: true},
			},
			wantSeverity: safety.Safe,
			wantReason:   "does not remove data or tighten constraints",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			assessments, err := safety.AssessRendered([]ast.Node{defaultChange(test.operation)}, platform.SQLServer)

			c.Assert(err, qt.IsNil)
			c.Assert(assessments, qt.HasLen, 1)
			c.Assert(assessments[0].Severity, qt.Equals, test.wantSeverity)
			c.Assert(assessments[0].Reason, qt.Equals, test.wantReason)
			// The words alone, which is how schema plan reads a statement,
			// say the same.
			words := safety.AssessSQL(assessments[0].Statement)
			c.Assert(words.Severity, qt.Equals, test.wantSeverity)
			c.Assert(words.Reason, qt.Equals, test.wantReason)
			c.Assert(safety.Classify(defaultChange(test.operation)), qt.Equals, test.wantSeverity)
		})
	}
}

// Only the DROP CONSTRAINT that reads its name from sys.default_constraints is
// a default's. One naming its constraint does not say what the constraint is,
// and one beside the default's in the same statement is read as it was.
func TestAssessSQL_SQLServerConstraintDropStaysDestructive(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{
			name:      "a constraint named",
			statement: "ALTER TABLE [dbo].[users] DROP CONSTRAINT [DF_users_status];",
		},
		{
			name: "a constraint beside the default's",
			statement: "EXEC sp_executesql N'DECLARE @drop nvarchar(max) = (SELECT N''ALTER TABLE [dbo].[users] DROP CONSTRAINT '' + QUOTENAME(dc.name)" +
				" FROM sys.default_constraints AS dc WHERE dc.parent_object_id = OBJECT_ID(N''[dbo].[users]'')" +
				" AND dc.parent_column_id = COLUMNPROPERTY(dc.parent_object_id, N''status'', ''ColumnId'')); EXEC (@drop);" +
				" ALTER TABLE [dbo].[users] DROP CONSTRAINT [users_status_check];';",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := safety.AssessSQL(test.statement)

			c.Assert(got.Severity, qt.Equals, safety.Destructive)
			c.Assert(got.Reason, qt.Equals, "DROP CONSTRAINT removes an existing data protection")
		})
	}
}
