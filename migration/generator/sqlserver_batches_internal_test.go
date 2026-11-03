package generator

// White-box testing required: every exported entry point that writes a native
// migration file reads a live database first, and the property under test --
// where a SQL Server statement ends in the file body -- is decided in the
// SQL-only generation stage below them.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlutil"
	"ptah.run/engine/builtin"
)

// sqlServerStatementBodyDiff adds a trigger whose body is a plain statement
// and, after it in the plan, a BEGIN ... END trigger.
func sqlServerStatementBodyDiff() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TriggersAdded: []difftypes.TriggerRef{
			{TriggerName: "trg_stmt", TableName: "t", Desired: schemamodel.Trigger{
				Name: "trg_stmt", Table: "t", Timing: "AFTER", Event: "INSERT", ForEach: "STATEMENT", Body: "SET NOCOUNT ON;",
			}},
			{TriggerName: "trg_block", TableName: "t", Desired: schemamodel.Trigger{
				Name: "trg_block", Table: "t", Timing: "AFTER", Event: "UPDATE", ForEach: "STATEMENT", Body: "BEGIN SET NOCOUNT ON; END",
			}},
		},
	}
}

// A SQL Server trigger's body runs to the end of its batch. Split as one
// script, the plan put the trigger after a statement-bodied one into that
// body, and the file ended the body's last statement with a second semicolon.
// Each statement now ends its batch with GO, and the file splits back into
// both triggers.
func TestGenerateUpMigrationSQL_EndsEverySQLServerBatch(t *testing.T) {
	c := qt.New(t)

	sql, err := upMigrationSQL(context.Background(), must.Must(builtin.New()),
		sqlServerStatementBodyDiff(), &schemamodel.Database{}, platform.SQLServer)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains,
		"CREATE TRIGGER [trg_stmt] ON [t] AFTER INSERT AS SET NOCOUNT ON;\nGO\n"+
			"CREATE TRIGGER [trg_block] ON [t] AFTER UPDATE AS BEGIN SET NOCOUNT ON; END;\nGO")
	c.Assert(sql, qt.Not(qt.Contains), ";;")
	c.Assert(sqlutil.SplitSQLStatementsForDialect(sql, platform.SQLServer), qt.HasLen, 2)
}
