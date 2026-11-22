package mssql

// White-box testing required: readFunctions is unexported, and the exported
// read asks a server for every catalog it describes. What a routine row
// reports about its result is a fact of the one sys.sql_modules query this
// test answers.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
)

// TestReadFunctions_FlagsATableValuedFunction pins that the read reports an
// inline table-valued function as returning a table, and a scalar function and
// a procedure as not. The comparison takes the TABLE list for a GRANT ALL on
// the first from that flag, and the FUNCTION or PROCEDURE list for the others.
func TestReadFunctions_FlagsATableValuedFunction(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringThreeRoutines)
	reader := NewSQLServerReader(db.SQL, "dbo")

	functions, err := reader.readFunctions(t.Context())

	c.Assert(err, qt.IsNil)
	flags := make(map[string]bool, len(functions))
	for _, function := range functions {
		flags[function.Name] = function.ReturnsTable
	}
	c.Assert(flags, qt.DeepEquals, map[string]bool{"tv": true, "sf": false, "p": false})
}

// answeringThreeRoutines is a server holding an inline table-valued function
// tv, a scalar function sf and a procedure p, with no parameters, and whose
// sys.sql_modules rows carry the options of a routine created with none.
func answeringThreeRoutines(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if !strings.Contains(query, "JOIN sys.sql_modules") {
		return dbtest.QueryResult{}, nil
	}
	routine := func(name, objectType, definition string) []driver.Value {
		return []driver.Value{"dbo", name, objectType, definition, int64(0), "CALLER", "", false, false, false, false, int64(0), int64(0)}
	}
	return dbtest.QueryResult{
		Columns: []string{
			"schema_name", "name", "type", "definition", "encrypted", "execute_as", "principal",
			"is_schema_bound", "is_recompiled", "null_on_null_input", "uses_native_compilation", "inline_type", "is_inlineable",
		},
		Rows: [][]driver.Value{
			routine("p", "P ", "CREATE PROCEDURE dbo.p AS SELECT 1 AS one"),
			routine("sf", "FN", "CREATE FUNCTION dbo.sf() RETURNS int AS BEGIN RETURN 1; END"),
			routine("tv", "IF", "CREATE FUNCTION dbo.tv() RETURNS TABLE AS RETURN SELECT 1 AS one"),
		},
	}, nil
}
