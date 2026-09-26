package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/dbschematogo"
)

// identityTable is one catalog table with an id primary key and one column of
// its own, so a column joined to the wrong table shows.
func identityTable(schema, name, column string) catalog.Table {
	return catalog.Table{
		Name:   name,
		Schema: schema,
		Type:   "BASE TABLE",
		Columns: []catalog.Column{
			{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			{Name: column, DataType: "integer", IsNullable: "YES"},
		},
	}
}

// orphanedColumns is the key columnsByTable lists a column under when no table
// carries its struct name.
const orphanedColumns = "(no table)"

// columnsByTable lists each table's columns, joined the way every consumer of
// the model joins them: through the table's struct name.
func columnsByTable(database *schemamodel.Database) map[string][]string {
	columns := make(map[string][]string, len(database.Tables))
	joined := make(map[int]bool, len(database.Fields))
	for _, table := range database.Tables {
		columns[table.QualifiedName()] = make([]string, 0)
		for i, field := range database.Fields {
			if field.StructName == table.StructName {
				columns[table.QualifiedName()] = append(columns[table.QualifiedName()], field.Name)
				joined[i] = true
			}
		}
	}
	for i, field := range database.Fields {
		if !joined[i] {
			columns[orphanedColumns] = append(columns[orphanedColumns], field.StructName+"."+field.Name)
		}
	}
	return columns
}

// structNames lists each table's struct name by its qualified name.
func structNames(database *schemamodel.Database) map[string]string {
	names := make(map[string]string, len(database.Tables))
	for _, table := range database.Tables {
		names[table.QualifiedName()] = table.StructName
	}
	return names
}

// Two tables whose names derive one struct name stay two tables, each with its
// own columns. Sharing the struct name, they are one table to every consumer
// of the model: `schema inspect` describes each with the other's columns and
// `introspect` writes one Go type holding both (stokaro/ptah#3647).
func TestConvert_TableIdentity_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		tables []catalog.Table
		want   map[string][]string
	}{
		{
			name:   "a quoted mixed-case name beside its folded spelling",
			tables: []catalog.Table{identityTable("app", "Docs", "a"), identityTable("app", "docs", "b")},
			want:   map[string][]string{"app.Docs": {"id", "a"}, "app.docs": {"id", "b"}},
		},
		{
			name:   "a camel-case name beside its snake-case spelling",
			tables: []catalog.Table{identityTable("app", "orderItems", "a"), identityTable("app", "order_items", "b")},
			want:   map[string][]string{"app.orderItems": {"id", "a"}, "app.order_items": {"id", "b"}},
		},
		{
			name: "a schema-prefixed name beside an underscored table",
			tables: []catalog.Table{
				identityTable("app", "users", "a"), identityTable("public", "users", "b"),
				identityTable("public", "app_users", "c"),
			},
			want: map[string][]string{
				"app.users": {"id", "a"}, "public.users": {"id", "b"}, "public.app_users": {"id", "c"},
			},
		},
		{
			name:   "names that derive distinct struct names",
			tables: []catalog.Table{identityTable("app", "orders", "a"), identityTable("app", "customers", "b")},
			want:   map[string][]string{"app.orders": {"id", "a"}, "app.customers": {"id", "b"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database := dbschematogo.ConvertDBSchemaToGoSchema(&catalog.Database{Tables: test.tables}, "postgres")

			c.Assert(columnsByTable(database), qt.DeepEquals, test.want)
		})
	}
}

// The numbered struct name goes to the same table whatever order the catalog
// lists the tables in. PostgreSQL orders them by the database collation, which
// puts "Docs" before docs under C and after it under most locales, and a name
// that moved between servers would rename a generated Go type. A name no other
// table derives is kept as derived.
func TestConvert_TableIdentity_StructNamesDoNotFollowCatalogOrder(t *testing.T) {
	tests := []struct {
		name   string
		tables []catalog.Table
	}{
		{name: "Docs first", tables: []catalog.Table{
			identityTable("app", "Docs", "a"), identityTable("app", "docs", "b"), identityTable("app", "orders", "c"),
		}},
		{name: "docs first", tables: []catalog.Table{
			identityTable("app", "orders", "c"), identityTable("app", "docs", "b"), identityTable("app", "Docs", "a"),
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database := dbschematogo.ConvertDBSchemaToGoSchema(&catalog.Database{Tables: test.tables}, "postgres")

			c.Assert(structNames(database), qt.DeepEquals, map[string]string{
				"app.Docs": "Docs", "app.docs": "Docs2", "app.orders": "Orders",
			})
		})
	}
}

// An index reaches the table it is on through that table's struct name, so an
// index on the second of two colliding tables stays with it.
func TestConvert_TableIdentity_IndexStaysWithItsTable(t *testing.T) {
	c := qt.New(t)
	schema := &catalog.Database{
		Tables: []catalog.Table{identityTable("app", "Docs", "a"), identityTable("app", "docs", "b")},
		Indexes: []catalog.Index{{
			Name: "docs_b_idx", TableName: "docs", Schema: "app", Columns: []string{"b"},
		}},
	}

	database := dbschematogo.ConvertDBSchemaToGoSchema(schema, "postgres")

	c.Assert(database.Indexes, qt.HasLen, 1)
	c.Assert(database.Indexes[0].StructName, qt.Equals, structNames(database)["app.docs"])
	c.Assert(database.Indexes[0].StructName, qt.Not(qt.Equals), structNames(database)["app.Docs"])
}
