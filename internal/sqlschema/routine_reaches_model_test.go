package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/parser"
	"ptah.run/internal/sqlschema"
)

// routinesOf parses a schema and returns the routines the conversion produced.
func routinesOf(c *qt.C, dialect, sql string) []schemamodel.Function {
	c.Helper()

	statements, err := parser.NewParser(sql, parser.WithDialect(dialect)).Parse()
	c.Assert(err, qt.IsNil)
	database, err := sqlschema.ToDatabase(statements, dialect)
	c.Assert(err, qt.IsNil)
	schemamodel.Finalize(&database)
	return database.Functions
}

// TestToDatabase_ACreateProcedureReachesTheModel is the step the pipeline lost
// it at.
//
// CREATE FUNCTION arrives as a CreateFunctionNode and CREATE PROCEDURE as one
// of three dialect routine nodes. The conversion had a case for the first and
// none for the others, and the switch has no default, so a procedure was
// parsed, modelled by the parser, and dropped in silence. `ptah schema render`
// emitted the tables and exited 0 (stokaro/ptah#2435).
func TestToDatabase_ACreateProcedureReachesTheModel(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		sql      string
		wantName string
		wantKind string
	}{
		{
			name:     "postgres",
			dialect:  "postgres",
			sql:      "CREATE PROCEDURE p() LANGUAGE plpgsql AS $$ BEGIN UPDATE t SET c = 1; END; $$;",
			wantName: "p",
			wantKind: "procedure",
		},
		{
			name:     "sqlserver",
			dialect:  "sqlserver",
			sql:      "CREATE PROCEDURE p AS UPDATE t SET c = 1;",
			wantName: "p",
			wantKind: "procedure",
		},
		{
			name:     "mysql",
			dialect:  "mysql",
			sql:      "CREATE PROCEDURE p() BEGIN UPDATE t SET c = 1; END",
			wantName: "p",
			wantKind: "procedure",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			routines := routinesOf(c, test.dialect, test.sql)

			c.Assert(routines, qt.HasLen, 1)
			c.Assert(routines[0].Name, qt.Equals, test.wantName)
			c.Assert(routines[0].Kind, qt.Equals, test.wantKind)
			c.Assert(routines[0].Body, qt.Not(qt.Equals), "")
		})
	}
}

// TestToDatabase_ANonPostgresRoutineDoesNotClaimPLpgSQL keeps a diagnostic from
// describing a body that never said it.
//
// A routine reaching the model with no language is defaulted to plpgsql, and
// the SQL Server renderer then skipped it saying it "declares language
// plpgsql" -- a sentence about T-SQL that is simply untrue.
func TestToDatabase_ANonPostgresRoutineDoesNotClaimPLpgSQL(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
	}{
		{name: "sqlserver", dialect: "sqlserver", sql: "CREATE PROCEDURE p AS UPDATE t SET c = 1;"},
		{name: "mysql", dialect: "mysql", sql: "CREATE PROCEDURE p() BEGIN UPDATE t SET c = 1; END"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			routines := routinesOf(c, test.dialect, test.sql)

			c.Assert(routines, qt.HasLen, 1)
			c.Assert(routines[0].Language, qt.Not(qt.Equals), "plpgsql")
		})
	}
}

// TestToDatabase_ACreateFunctionStillReachesTheModel is the control.
//
// The path that already worked has to keep working, or the three new cases
// could be passing by having replaced it.
func TestToDatabase_ACreateFunctionStillReachesTheModel(t *testing.T) {
	c := qt.New(t)

	routines := routinesOf(c, "postgres",
		"CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$ BEGIN UPDATE t SET c = 1; END; $$;")

	c.Assert(routines, qt.HasLen, 1)
	c.Assert(routines[0].Name, qt.Equals, "f")
	c.Assert(routines[0].Language, qt.Equals, "plpgsql")
}
