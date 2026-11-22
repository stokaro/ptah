package catalogspelling_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/catalogspelling"
)

// storedRead is an Oracle read holding a table created quoted, its folded
// namesake, and a view, a materialized view, a sequence, a trigger and roles
// created quoted, with an index and a foreign key over the quoted table, a
// role's folded namesake, a grant to the quoted role on the quoted table, and
// a grant on a schema, whose name keeps its stored spelling.
func storedRead() catalog.Database {
	return catalog.Database{
		StoredNames: true,
		Tables: []catalog.Table{
			{Name: "orders", Schema: "APP", Columns: []catalog.Column{{Name: "ID"}, {Name: "qty"}}},
			{Name: "ORDERS", Schema: "APP", Columns: []catalog.Column{{Name: "ID"}}},
		},
		Indexes: []catalog.Index{{Name: "idx_qty", TableName: "orders", Columns: []string{"qty"}}},
		Constraints: []catalog.Constraint{{
			Name: "fk_orders", TableName: "ORDERS", ColumnName: "ID",
			ForeignTable: new("orders"), ForeignColumn: new("ID"),
		}},
		Views:           []catalog.View{{Name: "v1", Schema: "APP"}},
		MatViews:        []catalog.MaterializedView{{Name: "mv1", Schema: "APP"}},
		Sequences:       []catalog.Sequence{{Name: "s1", Schema: "APP"}},
		Triggers:        []catalog.Trigger{{Name: "t1", Schema: "APP", Table: "orders"}},
		Roles:           []catalog.Role{{Name: "reader"}, {Name: "READER"}},
		RolesOutOfScope: []catalog.Role{{Name: "writer"}},
		Grants: []catalog.Grant{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "orders"},
			{Role: "READER", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "app"},
		},
	}
}

// TestSpell_StoredOracleRead spells every name no unquoted name reaches with
// its quotes, leaves the names already in upper case and the schema alone, and
// clears StoredNames, so a second Spell changes nothing.
func TestSpell_StoredOracleRead(t *testing.T) {
	c := qt.New(t)
	read := storedRead()

	got := catalogspelling.Spell(read, "oracle")

	c.Assert(got.StoredNames, qt.IsFalse)
	c.Assert(got.Tables[0].Name, qt.Equals, `"orders"`)
	c.Assert(got.Tables[0].Schema, qt.Equals, "APP")
	c.Assert(got.Tables[0].Columns[0].Name, qt.Equals, "ID")
	c.Assert(got.Tables[0].Columns[1].Name, qt.Equals, `"qty"`)
	c.Assert(got.Tables[1].Name, qt.Equals, "ORDERS")
	c.Assert(got.Indexes[0].Name, qt.Equals, `"idx_qty"`)
	c.Assert(got.Indexes[0].TableName, qt.Equals, `"orders"`)
	c.Assert(got.Indexes[0].Columns, qt.DeepEquals, []string{`"qty"`})
	c.Assert(got.Constraints[0].Name, qt.Equals, `"fk_orders"`)
	c.Assert(got.Constraints[0].TableName, qt.Equals, "ORDERS")
	c.Assert(*got.Constraints[0].ForeignTable, qt.Equals, `"orders"`)
	c.Assert(*got.Constraints[0].ForeignColumn, qt.Equals, "ID")
	c.Assert(got.Views[0].Name, qt.Equals, `"v1"`)
	c.Assert(got.MatViews[0].Name, qt.Equals, `"mv1"`)
	c.Assert(got.Sequences[0].Name, qt.Equals, `"s1"`)
	c.Assert(got.Triggers[0].Name, qt.Equals, `"t1"`)
	c.Assert(got.Triggers[0].Table, qt.Equals, `"orders"`)
	c.Assert(got.Roles[0].Name, qt.Equals, `"reader"`)
	c.Assert(got.Roles[1].Name, qt.Equals, "READER")
	c.Assert(got.RolesOutOfScope[0].Name, qt.Equals, `"writer"`)
	c.Assert(got.Grants[0].Role, qt.Equals, `"reader"`)
	c.Assert(got.Grants[0].ObjectName, qt.Equals, `"orders"`)
	c.Assert(got.Grants[1].Role, qt.Equals, "READER")
	c.Assert(got.Grants[1].ObjectName, qt.Equals, "app")
	c.Assert(catalogspelling.Spell(got, "oracle"), qt.DeepEquals, got)
	// The caller's read is left as it was.
	c.Assert(read, qt.DeepEquals, storedRead())
}

// TestSpell_LeavesNamesAlone keeps a read as it is where its names are not
// the stored spelling, as in a schema predicted from a declaration, or where
// the dialect states no fold.
func TestSpell_LeavesNamesAlone(t *testing.T) {
	predicted := storedRead()
	predicted.StoredNames = false
	tests := []struct {
		name    string
		read    catalog.Database
		dialect string
	}{
		{name: "a read that does not report stored names", read: predicted, dialect: "oracle"},
		{name: "a dialect that states no fold", read: storedRead(), dialect: "postgres"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(catalogspelling.Spell(test.read, test.dialect), qt.DeepEquals, test.read)
		})
	}
}
