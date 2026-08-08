package postgres

// White-box testing required: the fix for stokaro/ptah#933 is that
// readEnumsForSchema and readFunctionsForSchema stamp the schema they were
// asked about onto every row they build, following the same outputSchema
// convention tables, views and domains already follow. Both methods are
// unexported, and the exported ReadSchema path reaches them only through a live
// server, so the schema they stamp has no other observation point in a unit
// test. The value under test is the field, not the SQL, so this file simulates
// only the two result sets those methods read.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// enumFunctionCatalog answers the two reads this file exercises with one row
// each, so the assertions are about the Schema field and nothing else.
func enumFunctionCatalog(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if strings.Contains(query, "pg_enum") {
		return dbtest.QueryResult{
			Columns: []string{"enum_name", "enum_value", "enumsortorder"},
			Rows:    [][]driver.Value{{"color", "r", int64(1)}},
		}, nil
	}
	return dbtest.QueryResult{
		Columns: []string{
			"function_name", "parameters", "returns", "language",
			"security", "volatility", "body", "comment",
		},
		Rows: [][]driver.Value{{
			"fn_app", "", "integer", "sql", "INVOKER", "VOLATILE", " SELECT 2 ", "",
		}},
	}, nil
}

// TestReadEnumsAndFunctionsForSchema_StampTheSchemaTheyWereAskedFor asserts the
// VALUE, not that the field compiles: a reader that left it empty would keep
// every existing test green while `--exclude app.color` went on matching
// nothing.
//
// The row shape is the outputSchema convention every other resource follows --
// blank for the connection's own schema, named otherwise -- so both directions
// are asserted here. A reader that stamped the schema unconditionally would
// turn the default-schema rows red; the pre-fix reader turns the non-default
// rows red.
func TestReadEnumsAndFunctionsForSchema_StampTheSchemaTheyWereAskedFor(t *testing.T) {
	tests := []struct {
		name       string
		schemas    []string
		readSchema string
		want       string
	}{
		{
			name:       "a second schema is named",
			schemas:    []string{"public", "app"},
			readSchema: "app",
			want:       "app",
		},
		{
			name:       "the connection's own schema stays blank",
			schemas:    []string{"public", "app"},
			readSchema: "public",
			want:       "",
		},
		{
			name:       "an unscoped reader leaves every schema blank",
			schemas:    nil,
			readSchema: "app",
			want:       "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, enumFunctionCatalog)
			reader := NewPostgreSQLReaderWithCapabilities(db.SQL, "public", capability.Postgres16())
			reader.SetSchemas(test.schemas)

			enums, err := reader.readEnumsForSchema(test.readSchema)
			c.Assert(err, qt.IsNil)
			c.Assert(enums, qt.HasLen, 1)
			c.Assert(enums[0].Name, qt.Equals, "color")
			c.Assert(enums[0].Schema, qt.Equals, test.want)

			functions, err := reader.readFunctionsForSchema(test.readSchema)
			c.Assert(err, qt.IsNil)
			c.Assert(functions, qt.HasLen, 1)
			c.Assert(functions[0].Name, qt.Equals, "fn_app")
			c.Assert(functions[0].Schema, qt.Equals, test.want)
		})
	}
}
