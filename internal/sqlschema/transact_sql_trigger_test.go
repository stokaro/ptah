package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// TestRead_TransactSQLTrigger_HappyPath reads the triggers `ptah db read`
// prints for a SQL Server database into the model the catalog reader gives the
// same triggers: the name without brackets or schema, the table in its schema,
// AFTER for FOR, the events joined by OR in the model's one order, a
// statement-level trigger, and the body after AS.
func TestRead_TransactSQLTrigger_HappyPath(t *testing.T) {
	c := qt.New(t)
	const document = "CREATE SCHEMA app;\nGO\n" +
		"CREATE TABLE [t] ([id] INT PRIMARY KEY);\n" +
		"CREATE TABLE [app].[u] ([id] INT PRIMARY KEY);\n" +
		"CREATE TRIGGER [trg_instead] ON [t] INSTEAD OF DELETE AS BEGIN INSERT INTO dbo.audit_log(msg) VALUES(N'd'); END;\n" +
		"CREATE OR ALTER TRIGGER [app].[trg_for] ON [app].[u] FOR UPDATE, INSERT AS\nSET NOCOUNT ON;\nGO\n"

	database, _, err := sqlschema.Read([]byte(document), platform.SQLServer)

	c.Assert(err, qt.IsNil)
	c.Assert(database.Triggers, qt.DeepEquals, []schemamodel.Trigger{
		{
			Name: "trg_instead", Table: "t", Timing: "INSTEAD OF", Event: "DELETE", ForEach: "STATEMENT",
			Body: "BEGIN INSERT INTO dbo.audit_log(msg) VALUES(N'd'); END",
		},
		{Name: "trg_for", Table: "app.u", Timing: "AFTER", Event: "INSERT OR UPDATE", ForEach: "STATEMENT", Body: "SET NOCOUNT ON;"},
	})
}
