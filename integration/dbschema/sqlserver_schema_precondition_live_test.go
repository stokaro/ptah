//go:build integration

package dbschema_test

import (
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestSQLServerLiveSchemaPrecondition is stokaro/ptah#1996 on the server that
// refuses it, and it is live for the reason the defect was invisible offline:
// the plan was well-formed, and only SQL Server knew that its first statement
// could not run.
//
//	CREATE TABLE [app].[widget] ([id] INT PRIMARY KEY);
//	Msg 2760: The specified schema name "app" either does not exist or you do
//	not have permission to use it.
//
// So the assertion is that the plan APPLIES, not that it contains a statement.
// The offline sweep in migration/planner pins the shape and the dialect list;
// this pins that the shape is the one the server accepts -- SQL Server has no
// CREATE SCHEMA IF NOT EXISTS, and CREATE SCHEMA must be the first statement of
// its batch, so the guarded EXEC form is not a style choice.
//
// The second apply is half the test. A precondition that ran unconditionally
// would still pass the first one and turn every later run into a change.
func TestSQLServerLiveSchemaPrecondition(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_pre_%d", time.Now().UnixNano())
	quoted := quoteSQLServerIdentifier(schemaName)
	defer func() {
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoted+".[widget]")
		_, _ = conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoted)
	}()

	declared := &goschema.Database{
		Schemas: []goschema.Schema{{Name: "dbo"}, {Name: schemaName}},
		Tables:  []goschema.Table{{StructName: "W", Name: "widget", Schema: schemaName}},
		Fields: []goschema.Field{
			{StructName: "W", Name: "id", Type: "INT", Primary: true},
			{StructName: "W", Name: "title", Type: "NVARCHAR(50)"},
		},
	}

	// The schema does not exist yet, so this is the plan the issue reported.
	statements := planSQLServerAgainstLive(c, conn, declared, schemaName)
	c.Assert(statements, qt.Not(qt.HasLen), 0)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// The table is where the declaration put it, which a plan that created the
	// schema somewhere else would not achieve.
	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(sqlServerTableNames(live.Tables), qt.DeepEquals, []string{"widget"})

	// And the same declaration is now a no-op.
	c.Assert(planSQLServerAgainstLive(c, conn, declared, schemaName), qt.HasLen, 0)

	// A SECOND object in the schema that now exists is where the guard earns
	// its place. SQL Server has no CREATE SCHEMA IF NOT EXISTS, and repeating
	// the statement answers `Msg 2714: There is already an object named 'app'
	// in the database` -- so an unguarded precondition would fail every apply
	// after the first one that touched a new schema.
	declared.Tables = append(declared.Tables,
		goschema.Table{StructName: "G", Name: "gadget", Schema: schemaName})
	declared.Fields = append(declared.Fields,
		goschema.Field{StructName: "G", Name: "id", Type: "INT", Primary: true})
	defer func() {
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoted+".[gadget]")
	}()

	second := planSQLServerAgainstLive(c, conn, declared, schemaName)
	c.Assert(second, qt.Not(qt.HasLen), 0)
	for _, statement := range second {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	final, err := dbschema.ReadSchemaWithSchemas(conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(sqlServerTableNames(final.Tables), qt.DeepEquals, []string{"gadget", "widget"})
}

// planSQLServerAgainstLive reads the schema and plans the declaration against
// it, through the shipping reader, comparator and planner.
func planSQLServerAgainstLive(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	declared *goschema.Database,
	schemaName string,
) []string {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{schemaName})
	c.Assert(err, qt.IsNil)

	// CompareWithDatabase, not CompareWithDatabaseInfo: SQL Server compares
	// names under the live catalog collation, and a snapshot that has not
	// resolved them shares one conservative conflict key the planner refuses to
	// act on.
	diff, err := schemadiff.CompareWithDatabase(c.Context(), conn, declared, live, nil)
	c.Assert(err, qt.IsNil)

	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, declared, conn.Info().Dialect)
	c.Assert(err, qt.IsNil)
	return statements
}

// sqlServerTableNames is the read's table list, for an assertion that says
// which table rather than how many.
func sqlServerTableNames(tables []dbschematypes.DBTable) []string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.Name)
	}
	return names
}
