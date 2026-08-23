package schemachange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/schemachange"
)

// Indexes were the family the canonical model had no object for at all, and
// #1663 says why they are not constraints that happen to have columns: an index
// name is unique within its TABLE on some targets and within its SCHEMA on
// others, so a model that assumed either would merge two indexes on one target
// and split one on another.

func TestAnIndexIsPlanned(t *testing.T) {
	tests := []struct {
		name          string
		description   *goschema.Database
		catalog       *dbschematypes.DBSchema
		wantOperation schemachange.Operation
		wantChanged   []string
	}{
		{
			name:          "declared and absent from the database",
			description:   indexedWidget(false, "a", "b"),
			catalog:       indexedWidgetCatalog(false),
			wantOperation: schemachange.Add,
		},
		{
			name:          "present and no longer declared",
			description:   indexedWidget(false),
			catalog:       indexedWidgetCatalog(false, "a", "b"),
			wantOperation: schemachange.Remove,
		},
		{
			name:          "declared over different columns",
			description:   indexedWidget(false, "a"),
			catalog:       indexedWidgetCatalog(false, "a", "b"),
			wantOperation: schemachange.Modify,
			wantChanged:   []string{"columns"},
		},
		{
			name:          "declared unique where the database has it plain",
			description:   indexedWidget(true, "a", "b"),
			catalog:       indexedWidgetCatalog(false, "a", "b"),
			wantOperation: schemachange.Modify,
			wantChanged:   []string{"uniqueness"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			changes := changesFor(c, test.description, test.catalog)

			c.Assert(changes, qt.HasLen, 1)
			c.Assert(string(changes[0].ID.Kind), qt.Equals, "index")
			c.Assert(changes[0].Operation, qt.Equals, test.wantOperation)
			c.Assert(changes[0].Changed, qt.DeepEquals, test.wantChanged)
			c.Assert(changes[0].Status, qt.Equals, schemachange.Planned)
		})
	}
}

// TestAnUnchangedIndexIsNotAChange is the control the rows above need: a
// comparison that reported a change for every index would satisfy all four and
// rebuild every index on every run.
func TestAnUnchangedIndexIsNotAChange(t *testing.T) {
	c := qt.New(t)

	changes := changesFor(c, indexedWidget(false, "a", "b"), indexedWidgetCatalog(false, "a", "b"))

	c.Assert(changes, qt.HasLen, 0)
}

// TestAKeyTheReaderCouldNotNameIsNotRebuilt pins the one property that is not
// symmetric.
//
// A MySQL functional key part -- `KEY idx ((b + 1))` -- has a NULL COLUMN_NAME
// in information_schema.STATISTICS, so the reader reports FEWER columns than
// the key has and says so. Comparing those against a declaration would plan a
// rebuild on every run for a key that never changed, and the rebuild would drop
// the part the reader could not see.
func TestAKeyTheReaderCouldNotNameIsNotRebuilt(t *testing.T) {
	c := qt.New(t)
	catalog := indexedWidgetCatalog(false, "a")
	catalog.Indexes[0].KeyPartsIncomplete = true

	changes := changesFor(c, indexedWidget(false, "a", "b"), catalog)

	c.Assert(changes, qt.HasLen, 0)
}

// TestAnIndexInAnUndescribedSchemaIsNotDropped is the coverage gate, the same
// rule every other family in this comparison carries.
func TestAnIndexInAnUndescribedSchemaIsNotDropped(t *testing.T) {
	c := qt.New(t)
	description := indexedWidget(false)
	description.NotDescribed = coverage.Set{}.WithObject(coverage.Schema, "public")

	changes := changesFor(c, description, indexedWidgetCatalog(false, "a", "b"))

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Remove)
	c.Assert(changes[0].Status, qt.Equals, schemachange.Undecidable)
	c.Assert(changes[0].Diagnostic, qt.Contains, "is not a request to drop it")
}

// indexedWidget is a table carrying an index over the given columns, or none
// when they are absent.
func indexedWidget(unique bool, columns ...string) *goschema.Database {
	description := describedTable(
		goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
		goschema.Field{StructName: "Widget", Name: "a", Type: "text", Nullable: true},
		goschema.Field{StructName: "Widget", Name: "b", Type: "text", Nullable: true},
	)
	return withDeclaredIndex(description, unique, columns)
}

func withDeclaredIndex(
	description *goschema.Database,
	unique bool,
	columns []string,
) *goschema.Database {
	return map[bool]func() *goschema.Database{
		true: func() *goschema.Database { return description },
		false: func() *goschema.Database {
			description.Indexes = append(description.Indexes, goschema.Index{
				StructName: "Widget", Name: "idx_widget_ab", Fields: columns, Unique: unique,
			})
			return description
		},
	}[len(columns) == 0]()
}

// indexedWidgetCatalog is that table as a catalog read reports it.
func indexedWidgetCatalog(unique bool, columns ...string) *dbschematypes.DBSchema {
	catalog := catalogTable(
		dbschematypes.DBColumn{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
		dbschematypes.DBColumn{Name: "a", DataType: "text", IsNullable: "YES"},
		dbschematypes.DBColumn{Name: "b", DataType: "text", IsNullable: "YES"},
	)
	return withCatalogIndex(catalog, unique, columns)
}

func withCatalogIndex(
	catalog *dbschematypes.DBSchema,
	unique bool,
	columns []string,
) *dbschematypes.DBSchema {
	return map[bool]func() *dbschematypes.DBSchema{
		true: func() *dbschematypes.DBSchema { return catalog },
		false: func() *dbschematypes.DBSchema {
			catalog.Indexes = append(catalog.Indexes, dbschematypes.DBIndex{
				Name: "idx_widget_ab", TableName: "widget", Schema: "public",
				Columns: columns, IsUnique: unique,
			})
			return catalog
		},
	}[len(columns) == 0]()
}
