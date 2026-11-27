package atlasschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/safety"
)

// TestPlanFileWithStatementsFromSQL_SQLServerRatesTheStatementsItRuns drives
// the path that joins the statement splitter and the safety judgment: an edited
// plan holds the pieces `migrations up` sends for the same text, and each is
// rated by every statement the server runs from it. A T-SQL statement needs no
// semicolon before the next one, so judged by the words a piece starts with,
// the TRUNCATE after a CREATE TABLE and the DROP TABLE inside an IF read as
// safe, and the edited plan keeps a destructive=false marker.
func TestPlanFileWithStatementsFromSQL_SQLServerRatesTheStatementsItRuns(t *testing.T) {
	c := qt.New(t)
	edited := "CREATE TABLE dbo.a (id int) TRUNCATE TABLE dbo.x;\n" +
		"IF OBJECT_ID(N'dbo.y') IS NOT NULL BEGIN DROP TABLE dbo.y; END;\n" +
		"MERGE dbo.a AS t USING (SELECT 1 AS id) AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);\n"
	const safe = "does not remove data or tighten constraints"

	plan := withStatements(c, atlasschema.PlanFile{Dialect: "sqlserver"}, edited)

	c.Assert(plan.Statements, qt.DeepEquals, []atlasschema.PlanStatement{
		{SQL: "CREATE TABLE dbo.a (id int) TRUNCATE TABLE dbo.x", Severity: safety.Destructive, Reason: "TRUNCATE removes all rows from a table"},
		{SQL: "IF OBJECT_ID(N'dbo.y') IS NOT NULL BEGIN DROP TABLE dbo.y; END", Severity: safety.Destructive, Reason: "DROP TABLE removes the table and all rows"},
		{SQL: "MERGE dbo.a AS t USING (SELECT 1 AS id) AS s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id);", Severity: safety.Safe, Reason: safe},
	})
	c.Assert(plan.Destructive, qt.IsTrue)
	sent := make([]string, 0, len(plan.Statements))
	for _, statement := range plan.Statements {
		sent = append(sent, statement.SQL)
	}
	c.Assert(sent, qt.DeepEquals, sqlutil.SplitSQLStatementsForDialect(edited, "sqlserver"))
}
