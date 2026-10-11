package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/dialect/mssql/mssqllint"
	"ptah.run/engine/builtin"
	"ptah.run/migration/safety"
)

// A PolyBase drop on SQL Server removes no data SQL Server stores, yet every
// reader of the external object stops working, so the SQL Server owner judges
// it a warning, with its own reason, whether the statement is read alone or
// rendered from a plan. An ordinary DROP TABLE beside it stays destructive by
// its words.
func TestAssess_PolyBaseDropsOnSQLServer(t *testing.T) {
	tests := []struct {
		statement string
		severity  safety.Severity
		reason    string
	}{
		{statement: "DROP EXTERNAL TABLE [dbo].[events]", severity: safety.Warning, reason: mssqllint.DropExternalTableReason},
		{statement: "DROP EXTERNAL DATA SOURCE [lake]", severity: safety.Warning, reason: mssqllint.DropExternalDataSourceReason},
		{statement: "DROP TABLE [dbo].[orders]", severity: safety.Destructive, reason: "DROP TABLE removes the table and all rows"},
	}
	for _, test := range tests {
		t.Run(test.statement, func(t *testing.T) {
			c := qt.New(t)

			text := assessText(c, platform.SQLServer, test.statement)
			rendered, err := safety.AssessRendered(c.Context(), must.Must(builtin.New()), []ast.Node{ast.NewRawSQL(test.statement + ";")}, platform.SQLServer)

			c.Assert(text.Severity, qt.Equals, test.severity)
			c.Assert(text.Reason, qt.Equals, test.reason)
			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.HasLen, 1)
			c.Assert(rendered[0].Severity, qt.Equals, test.severity)
			c.Assert(rendered[0].Reason, qt.Equals, test.reason)
		})
	}
}
