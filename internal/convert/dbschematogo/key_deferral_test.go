package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// deferredKeySchema is table slots with a primary key over id and a UNIQUE
// over pos, both under the names PostgreSQL gives them, both deferring as
// given.
func deferredKeySchema(deferrable bool, initially string) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{
			Name: "slots", Type: "BASE TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "pos", DataType: "integer", IsNullable: "YES", IsUnique: true},
			},
		}},
		Constraints: []catalog.Constraint{
			{
				TableName: "slots", Name: "slots_pkey", Type: "PRIMARY KEY", ColumnNames: []string{"id"},
				Deferrable: deferrable, Initially: initially,
			},
			{
				TableName: "slots", Name: "slots_pos_key", Type: "UNIQUE", ColumnNames: []string{"pos"},
				Deferrable: deferrable, Initially: initially,
			},
		},
	}
}

// TestConvert_DescribesADeferrableKey keeps a key's deferral in the
// description. A single-column key under the server's name is written on its
// column, which has no deferral: a deferrable one is written on the table, as
// a key with an INCLUDE payload is.
func TestConvert_DescribesADeferrableKey(t *testing.T) {
	c := qt.New(t)

	database := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), deferredKeySchema(true, "deferred"), "postgres", must.Must(builtin.New())))

	c.Assert(database.Tables[0].PrimaryKey, qt.DeepEquals, []string{"id"})
	c.Assert(database.Tables[0].PrimaryKeyDeferrable, qt.IsTrue)
	c.Assert(database.Tables[0].PrimaryKeyInitially, qt.Equals, "deferred")
	c.Assert(database.Constraints, qt.HasLen, 1)
	c.Assert(database.Constraints[0].Name, qt.Equals, "slots_pos_key")
	c.Assert(database.Constraints[0].Deferrable, qt.IsTrue)
	c.Assert(database.Constraints[0].Initially, qt.Equals, "deferred")
	c.Assert(fieldNamed(c, database, "pos").Unique, qt.IsFalse)
}

// TestConvert_LeavesAKeyThatDoesNotDeferToItsColumn is the control.
func TestConvert_LeavesAKeyThatDoesNotDeferToItsColumn(t *testing.T) {
	c := qt.New(t)

	database := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), deferredKeySchema(false, ""), "postgres", must.Must(builtin.New())))

	c.Assert(database.Tables[0].PrimaryKey, qt.IsNil)
	c.Assert(database.Tables[0].PrimaryKeyDeferrable, qt.IsFalse)
	c.Assert(database.Constraints, qt.HasLen, 0)
	c.Assert(fieldNamed(c, database, "pos").Unique, qt.IsTrue)
}

// fieldNamed returns the converted column called name.
func fieldNamed(c *qt.C, database *schemamodel.Database, name string) schemamodel.Field {
	c.Helper()
	for _, field := range database.Fields {
		if field.Name == name {
			return field
		}
	}
	c.Fatalf("no field %s in %+v", name, database.Fields)
	return schemamodel.Field{}
}
