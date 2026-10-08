package clickhouse_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// eventsTable declares one `events` table whose schema is spelled as given.
func eventsTable(tableSchema string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Event", Name: "events", Schema: tableSchema}},
		Fields: []schemamodel.Field{
			{StructName: "Event", Name: "id", Type: "UInt64", Primary: true},
			{StructName: "Event", Name: "note", Type: "String"},
		},
	}
}

// Captured operands and the diff use the same connection-derived identity,
// including an omitted default database. No declaration lookup is needed.
func TestColumnDDLResolvesTheTableAcrossSchemaSpellings(t *testing.T) {
	tests := []struct {
		name        string
		tableSchema string
		diffName    string
	}{
		{
			// Control: both sides already agree.
			name:        "both sides spell the table the same way",
			tableSchema: "app",
			diffName:    "app.events",
		},
		{
			// ClickHouse's schema IS its database, so identifier.ForDialect
			// leaves DefaultSchema empty and only the unqualified tier joins
			// these two.
			name:        "the diff names the database and the declaration does not",
			tableSchema: "",
			diffName:    "app.events",
		},
		{
			name:        "the declaration names the database and the diff does not",
			tableSchema: "app",
			diffName:    "events",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			semantics := identifier.ForDialect("clickhouse")
			semantics.DefaultSchema = "app"
			change := capturedColumnFixture(eventsTable(test.tableSchema).Tables[0])
			change.TableName = test.diffName
			change.ColumnsAdded = difftypes.ColumnChanges{{StructName: "Event", Name: "note", Type: "String"}}
			statements, err := planner.GenerateSchemaDiffSQLStatements(
				t.Context(), must.Must(builtin.New()),
				&difftypes.SchemaDiff{IdentifierSemantics: &semantics, TablesModified: []difftypes.TableDiff{change}}, "clickhouse",
			)

			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(plan, qt.Contains, "ADD COLUMN", qt.Commentf("plan:\n%s", plan))
			c.Assert(plan, qt.Not(qt.Contains), "could not find struct", qt.Commentf("plan:\n%s", plan))
		})
	}
}

// TestColumnDDLDoesNotGuessBetweenSchemas is the control: the statement is
// rendered against the DIFF's name, so resolving a declaration in another
// database would write the column onto a table the desired schema never
// declared.
func TestColumnDDLDoesNotGuessBetweenSchemas(t *testing.T) {
	c := qt.New(t)

	change := capturedColumnFixture(eventsTable("reporting").Tables[0])
	change.TableName = "app.events"
	change.ColumnsAdded = difftypes.ColumnChanges{{StructName: "Event", Name: "note", Type: "String"}}
	statements, err := planner.GenerateSchemaDiffSQLStatements(
		t.Context(), must.Must(builtin.New()),
		&difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{change}}, "clickhouse",
	)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(statements, qt.HasLen, 0)
}

// TestCreateTableResolvesTheTableAcrossSchemaSpellings pins addNewTables, which
// the sweep left as a raw map keyed by `table.QualifiedName()` even though its
// direct SQLite sibling was converted.
//
// It is the worst shape in this family: a table in TablesAdded whose declaration
// spells the database differently got NO CREATE TABLE -- no statement, no
// comment -- and every later ALTER against it then fails at apply time.
func TestCreateTableResolvesTheTableAcrossSchemaSpellings(t *testing.T) {
	tests := []struct {
		name        string
		tableSchema string
		diffName    string
	}{
		{
			// Control: both sides already agree.
			name:        "both sides spell the table the same way",
			tableSchema: "app",
			diffName:    "app.events",
		},
		{
			name:        "the diff names the database and the declaration does not",
			tableSchema: "",
			diffName:    "app.events",
		},
		{
			name:        "the declaration names the database and the diff does not",
			tableSchema: "app",
			diffName:    "events",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			// The creation carries the declaration, and its Name carries the
			// spelling this row is about: the plan renders what the creation
			// holds, and the diff naming the table differently is the subject.
			declared := eventsTable(test.tableSchema)
			creations := difftypes.TableCreationsFor(declared, identifier.ForDialect("clickhouse"), "events")
			creations[0].Name = test.diffName
			statements, err := planner.GenerateSchemaDiffSQLStatements(
				context.Background(), must.Must(builtin.New()),
				&difftypes.SchemaDiff{TablesAdded: creations},

				"clickhouse",
			)

			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(plan, qt.Contains, "CREATE TABLE", qt.Commentf("plan:\n%s", plan))
		})
	}
}

// TestCreateTableDoesNotGuessBetweenSchemas is the control for the same site: a
// table declared in one database is not the table a diff creates in another, and
// creating it would put the relation in the wrong place.
func TestCreateTableDoesNotGuessBetweenSchemas(t *testing.T) {
	c := qt.New(t)

	statements, err := planner.GenerateSchemaDiffSQLStatements(
		context.Background(), must.Must(builtin.New()),
		&difftypes.SchemaDiff{TablesAdded: difftypes.TableCreationsFor(eventsTable("reporting"), identifier.ForDialect("clickhouse"), "app.events")},

		"clickhouse",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "CREATE TABLE")
}
