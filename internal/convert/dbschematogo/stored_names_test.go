package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// TestConvertDBSchemaToGoSchema_StoredNames converts an Oracle read holding a
// table created quoted beside its folded namesake. A read reports stored
// names, so the converted schema names the quoted table `"orders"`, which
// renders and compares as that table rather than as ORDERS, and keeps the two
// tables apart. The same rows without the mark are a schema predicted from a
// declaration and keep their names as written (stokaro/ptah#6243).
func TestConvertDBSchemaToGoSchema_StoredNames(t *testing.T) {
	tables := []catalog.Table{
		{Name: "orders", Schema: "APP", Columns: []catalog.Column{{Name: "ID", DataType: "NUMBER", IsNullable: "NO"}, {Name: "qty", DataType: "NUMBER", IsNullable: "YES"}}},
		{Name: "ORDERS", Schema: "APP", Columns: []catalog.Column{{Name: "ID", DataType: "NUMBER", IsNullable: "NO"}}},
	}
	tests := []struct {
		name        string
		storedNames bool
		tables      []string
		fields      []string
	}{
		{name: "a read", storedNames: true, tables: []string{`"orders"`, "ORDERS"}, fields: []string{"ID", `"qty"`, "ID"}},
		{name: "a predicted schema", tables: []string{"orders", "ORDERS"}, fields: []string{"ID", "qty", "ID"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := &catalog.Database{StoredNames: test.storedNames, Tables: tables}

			database, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), source, "oracle", must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			names := make([]string, 0, len(database.Tables))
			for _, table := range database.Tables {
				names = append(names, table.Name)
			}
			fields := make([]string, 0, len(database.Fields))
			for _, field := range database.Fields {
				fields = append(fields, field.Name)
			}
			c.Assert(names, qt.DeepEquals, test.tables)
			c.Assert(fields, qt.DeepEquals, test.fields)
			c.Assert(source.Tables[0].Name, qt.Equals, "orders")
		})
	}
}
