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
// table and a role created quoted beside their folded namesakes, and a grant
// to the quoted role on the quoted table. A read reports stored names, so the
// converted schema names the quoted table `"orders"` and the quoted role
// `"reader"`, which render and compare as those objects rather than as ORDERS
// and READER, and keeps each pair apart. The same rows without the mark are a
// schema predicted from a declaration and keep their names as written
// (stokaro/ptah#6243).
func TestConvertDBSchemaToGoSchema_StoredNames(t *testing.T) {
	tables := []catalog.Table{
		{Name: "orders", Schema: "APP", Columns: []catalog.Column{{Name: "ID", DataType: "NUMBER", IsNullable: "NO"}, {Name: "qty", DataType: "NUMBER", IsNullable: "YES"}}},
		{Name: "ORDERS", Schema: "APP", Columns: []catalog.Column{{Name: "ID", DataType: "NUMBER", IsNullable: "NO"}}},
	}
	roles := []catalog.Role{{Name: "reader"}, {Name: "READER"}}
	grants := []catalog.Grant{{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", Schema: "APP", ObjectName: "orders"}}
	tests := []struct {
		name        string
		storedNames bool
		tables      []string
		fields      []string
		roles       []string
		grants      []string
	}{
		{
			name: "a read", storedNames: true, tables: []string{`"orders"`, "ORDERS"}, fields: []string{"ID", `"qty"`, "ID"},
			roles: []string{`"reader"`, "READER"}, grants: []string{`"reader" ON APP."""orders"""`},
		},
		{
			name: "a predicted schema", tables: []string{"orders", "ORDERS"}, fields: []string{"ID", "qty", "ID"},
			roles: []string{"reader", "READER"}, grants: []string{"reader ON APP.orders"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := &catalog.Database{StoredNames: test.storedNames, Tables: tables, Roles: roles, Grants: grants}

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
			declaredRoles := make([]string, 0, len(database.Roles))
			for _, role := range database.Roles {
				declaredRoles = append(declaredRoles, role.Name)
			}
			declaredGrants := make([]string, 0, len(database.Grants))
			for _, grant := range database.Grants {
				declaredGrants = append(declaredGrants, grant.Role+" ON "+grant.OnTable)
			}
			c.Assert(names, qt.DeepEquals, test.tables)
			c.Assert(fields, qt.DeepEquals, test.fields)
			c.Assert(declaredRoles, qt.DeepEquals, test.roles)
			c.Assert(declaredGrants, qt.DeepEquals, test.grants)
			c.Assert(source.Tables[0].Name, qt.Equals, "orders")
		})
	}
}
