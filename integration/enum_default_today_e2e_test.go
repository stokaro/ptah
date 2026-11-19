//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestApplyAnEnumDefaultNamedTodayE2E applies, on PostgreSQL, a SQL schema and
// a Go model whose column is typed by an enum named update_kind with the
// default 'today', twice, and reads the value a new row takes. The server
// reads the string as the enum's value, not as a moment, so the table is
// created and the second apply plans nothing.
func TestApplyAnEnumDefaultNamedTodayE2E(t *testing.T) {
	rows := []struct {
		name  string
		model declaredModel
	}{
		{
			name: "a SQL schema",
			model: declaredModel{
				source: "CREATE TYPE update_kind AS ENUM ('today', 'later');\n" +
					"CREATE TABLE notes (id int PRIMARY KEY, kind update_kind DEFAULT 'today');\n",
				file: "schema.sql",
				flag: "--schema-file",
			},
		},
		{
			name: "a Go model",
			model: declaredModel{
				source: "package entities\n\n" +
					"//ptah:schema:enum name=\"update_kind\" values=\"today,later\"\n" +
					"type UpdateKind struct{}\n\n" +
					"//ptah:schema:table name=\"notes\"\n" +
					"type Note struct {\n" +
					"\t//ptah:schema:field name=\"id\" type=\"INT\" primary=\"true\"\n" +
					"\tID int\n" +
					"\t//ptah:schema:field name=\"kind\" type=\"update_kind\" default=\"today\"\n" +
					"\tKind string\n" +
					"}\n",
				file: "notes.go",
				flag: "--root-dir",
			},
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := postgresScratchDevURL(c, "enum_today")

			got := applyEscapedLiteralModel(c, database, row.model, "SELECT kind::text FROM notes")

			c.Assert(got, qt.Equals, "today")
		})
	}
}
