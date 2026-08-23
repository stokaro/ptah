package schemachange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/schemachange"
)

// PostgreSQL, MySQL and MariaDB enforce a UNIQUE constraint with an index of the
// constraint's own name on the constraint's own table, and introspection reports
// that ONE object twice -- once in the index catalog, once in the constraint
// catalog. On MySQL and MariaDB there is not even a separate notion to report:
//
//	ALTER TABLE widget ADD CONSTRAINT uq_widget_code UNIQUE (code)
//	CREATE UNIQUE INDEX uq_widget_code ON widget (code)
//
// produce the same row (stokaro/ptah#1286).
//
// Reading both as objects made a desired state that declares the CONSTRAINT look
// like one that dropped the INDEX, and the plan came out as
// `DROP INDEX IF EXISTS "public"."uq_widget_code"` -- which drops the constraint
// with it.

func TestAConstraintsBackingIndexIsNotDropped(t *testing.T) {
	c := qt.New(t)

	changes := changesFor(c, widgetDeclaringUniqueConstraint(), widgetReportingBoth())

	c.Assert(changes, qt.HasLen, 0)
}

// TestAStandaloneUniqueIndexIsStillCompared is the control, and it is the case
// the suppression must not swallow.
//
// A schema that really does declare a unique INDEX has one, and the server
// creates no constraint beside it -- so nothing suppresses it, and a database
// missing it still gets one.
func TestAStandaloneUniqueIndexIsStillCompared(t *testing.T) {
	c := qt.New(t)
	description := describedTable(
		goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
		goschema.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true},
	)
	description.Indexes = append(description.Indexes, goschema.Index{
		StructName: "Widget", Name: "uq_widget_code", Fields: []string{"code"}, Unique: true,
	})

	changes := changesFor(c, description, widgetWithoutTheIndex())

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(string(changes[0].ID.Kind), qt.Equals, "index")
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Add)
}

// TestAnIndexThatBacksNothingIsStillDropped is the second control: the
// suppression must apply to a constraint's index and to nothing else, or an
// index the desired state really did remove would survive forever.
func TestAnIndexThatBacksNothingIsStillDropped(t *testing.T) {
	c := qt.New(t)
	catalog := widgetWithoutTheIndex()
	catalog.Indexes = []dbschematypes.DBIndex{{
		Name: "idx_widget_code", TableName: "widget", Schema: "public",
		Columns: []string{"code"},
	}}

	changes := changesFor(c, widgetDeclaringNothing(), catalog)

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(string(changes[0].ID.Kind), qt.Equals, "index")
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Remove)
}

// widgetDeclaringUniqueConstraint declares the guarantee as a CONSTRAINT and no
// index, which is what a schema author writes.
func widgetDeclaringUniqueConstraint() *goschema.Database {
	description := widgetDeclaringNothing()
	description.Constraints = append(description.Constraints, goschema.Constraint{
		StructName: "Widget", Name: "uq_widget_code", Type: "UNIQUE", Columns: []string{"code"},
	})
	return description
}

func widgetDeclaringNothing() *goschema.Database {
	return describedTable(
		goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
		goschema.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true},
	)
}

// widgetReportingBoth is the catalog as every one of those engines reports it:
// the constraint AND the index it is enforced with, one object twice.
func widgetReportingBoth() *dbschematypes.DBSchema {
	catalog := widgetWithoutTheIndex()
	catalog.Constraints = []dbschematypes.DBConstraint{{
		Name: "uq_widget_code", TableName: "widget", Schema: "public",
		Type: "UNIQUE", ColumnNames: []string{"code"},
	}}
	catalog.Indexes = []dbschematypes.DBIndex{{
		Name: "uq_widget_code", TableName: "widget", Schema: "public",
		Columns: []string{"code"}, IsUnique: true,
	}}
	return catalog
}

func widgetWithoutTheIndex() *dbschematypes.DBSchema {
	return catalogTable(
		dbschematypes.DBColumn{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
		dbschematypes.DBColumn{Name: "code", DataType: "text", IsNullable: "YES"},
	)
}

// TestAPrimaryKeysBackingIndexIsNotDropped is the same fact for the other
// constraint kind that owns an index, and it is the one the first version of
// this suppression got wrong: it asked whether the constraint was a standalone
// UNIQUE, so `widget_pkey` stayed unsuppressed and the plan dropped it.
func TestAPrimaryKeysBackingIndexIsNotDropped(t *testing.T) {
	c := qt.New(t)
	description := describedTableWithKey([]string{"id"},
		goschema.Field{StructName: "Widget", Name: "id", Type: "int"},
		goschema.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true},
	)
	catalog := widgetWithoutTheIndex()
	catalog.Constraints = []dbschematypes.DBConstraint{{
		Name: "widget_pkey", TableName: "widget", Schema: "public",
		Type: "PRIMARY KEY", ColumnNames: []string{"id"},
	}}
	catalog.Indexes = []dbschematypes.DBIndex{{
		Name: "widget_pkey", TableName: "widget", Schema: "public",
		Columns: []string{"id"}, IsUnique: true, IsPrimary: true,
	}}

	changes := changesFor(c, description, catalog)

	c.Assert(changes, qt.HasLen, 0)
}

// TestSuppressionMatchesTheNameAndTheTable pins what makes two rows one object.
//
// Both halves are load-bearing and neither is visible from the rows above, where
// the constraint and the index always agree on both: an index that merely shares
// a table with a constraint, and one that merely shares a NAME with a constraint
// on another table, are each an index of their own and are each still dropped.
//
// The second case is a shape the engines allow: an index name is unique per
// schema, a constraint name only per table, so `uq_widget_code` can name gadget's
// constraint and widget's index at once.
func TestSuppressionMatchesTheNameAndTheTable(t *testing.T) {
	tests := []struct {
		name           string
		constraintName string
		indexName      string
		indexTable     string
	}{
		{
			name:           "the same table, another name",
			constraintName: "uq_widget_code",
			indexName:      "idx_widget_code",
			indexTable:     "widget",
		},
		{
			name:           "the same name, another table",
			constraintName: "uq_widget_code",
			indexName:      "uq_widget_code",
			indexTable:     "widget",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			constraintTable := "widget"
			if test.indexName == test.constraintName {
				constraintTable = "gadget"
			}

			catalog := widgetWithoutTheIndex()
			catalog.Tables = append(catalog.Tables, dbschematypes.DBTable{
				Name: "gadget", Schema: "public", Type: "BASE TABLE",
				Columns: []dbschematypes.DBColumn{
					{Name: "code", DataType: "text", IsNullable: "YES"},
				},
			})
			catalog.Constraints = []dbschematypes.DBConstraint{{
				Name: test.constraintName, TableName: constraintTable, Schema: "public",
				Type: "UNIQUE", ColumnNames: []string{"code"},
			}}
			catalog.Indexes = []dbschematypes.DBIndex{{
				Name: test.indexName, TableName: test.indexTable, Schema: "public",
				Columns: []string{"code"},
			}}

			description := widgetDeclaringNothing()
			description.Tables = append(description.Tables,
				goschema.Table{StructName: "Gadget", Name: "gadget"})
			description.Fields = append(description.Fields,
				goschema.Field{StructName: "Gadget", Name: "code", Type: "text", Nullable: true})
			description.Constraints = append(description.Constraints, goschema.Constraint{
				StructName: structOf(constraintTable), Name: test.constraintName,
				Type: "UNIQUE", Columns: []string{"code"},
			})

			changes := changesFor(c, description, catalog)

			c.Assert(changes, qt.HasLen, 1)
			c.Assert(string(changes[0].ID.Kind), qt.Equals, "index")
			c.Assert(changes[0].Operation, qt.Equals, schemachange.Remove)
			c.Assert(changes[0].ID.Name.Source, qt.Equals, test.indexName)
		})
	}
}

func structOf(table string) string {
	if table == "gadget" {
		return "Gadget"
	}
	return "Widget"
}
