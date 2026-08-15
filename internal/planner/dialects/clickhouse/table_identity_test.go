package clickhouse_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// eventsTable declares one `events` table whose schema is spelled as given.
func eventsTable(tableSchema string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "Event", Name: "events", Schema: tableSchema}},
		Fields: []goschema.Field{
			{StructName: "Event", Name: "id", Type: "UInt64", Primary: true},
			{StructName: "Event", Name: "note", Type: "String"},
		},
	}
}

// TestColumnDDLResolvesTheTableAcrossSchemaSpellings pins clickhouse's
// lookupStructName.
//
// Reverting it to the raw `QualifiedName() == tableName` loop left the whole
// suite green. The failure is not silent here -- the planner emits
// `WARNING: ClickHouse planner could not find struct for table …` -- but a
// comment is not a column, and the plan applies cleanly having changed nothing.
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
			statements, err := planner.GenerateSchemaDiffSQLStatements(
				&types.SchemaDiff{TablesModified: []types.TableDiff{{
					TableName:    test.diffName,
					ColumnsAdded: []string{"note"},
				}}},
				eventsTable(test.tableSchema),
				"clickhouse",
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

	statements, err := planner.GenerateSchemaDiffSQLStatements(
		&types.SchemaDiff{TablesModified: []types.TableDiff{{
			TableName:    "app.events",
			ColumnsAdded: []string{"note"},
		}}},
		eventsTable("reporting"),
		"clickhouse",
	)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "ADD COLUMN")
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
			statements, err := planner.GenerateSchemaDiffSQLStatements(
				&types.SchemaDiff{TablesAdded: []string{test.diffName}},
				eventsTable(test.tableSchema),
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
		&types.SchemaDiff{TablesAdded: []string{"app.events"}},
		eventsTable("reporting"),
		"clickhouse",
	)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "CREATE TABLE")
}
