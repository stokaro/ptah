package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/sqlschema"
)

// TestToDBSchema_KeepsTheKindOfARoutineFromAFile reads a schema file that
// holds a procedure and a function, as `schema diff` reads its --from side,
// and converts it to the state the comparison reads. A procedure the
// conversion leaves without its kind reads as a function there, and the plan
// that removes it writes DROP FUNCTION, which SQL Server refuses for a
// procedure (stokaro/ptah#6740).
func TestToDBSchema_KeepsTheKindOfARoutineFromAFile(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    map[string]string
	}{
		{
			name:    "SQL Server",
			dialect: platform.SQLServer,
			sql: "CREATE PROCEDURE [MixedCase] AS BEGIN SELECT 1 END;\nGO\n" +
				"CREATE FUNCTION [f1]() RETURNS int AS BEGIN RETURN 1 END;\nGO\n",
			want: map[string]string{"MixedCase": schemamodel.FunctionKindProcedure, "f1": schemamodel.FunctionKindFunction},
		},
		{
			name:    "PostgreSQL",
			dialect: platform.Postgres,
			sql: "CREATE PROCEDURE p1() LANGUAGE sql AS $$ SELECT 1 $$;\n" +
				"CREATE FUNCTION f1() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;\n",
			want: map[string]string{"p1": schemamodel.FunctionKindProcedure, "f1": ""},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			document, _, err := sqlschema.Read([]byte(test.sql), test.dialect)
			c.Assert(err, qt.IsNil)
			schemamodel.Finalize(&document)

			state, err := goschematodb.ToDBSchema(c.Context(), &document, test.dialect, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(routineKinds(state.Functions), qt.DeepEquals, test.want)
		})
	}
}

// routineKinds is the kind of each routine by its name.
func routineKinds(functions []catalog.Function) map[string]string {
	kinds := make(map[string]string, len(functions))
	for _, function := range functions {
		kinds[function.Name] = function.Kind
	}
	return kinds
}
