package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/migration/schemadiff"
)

// twoKeysOverOneColumn is what PostgreSQL 18.6 reads back for
// `CREATE TABLE ord (FOREIGN KEY (c) REFERENCES op (id) ON DELETE CASCADE,
// c int REFERENCES op (id))`: ord_c_fkey with the action and ord_c_fkey1
// without. names lists the keys in the order the catalog reports them.
func twoKeysOverOneColumn(names ...string) *catalog.Database {
	cascade, noAction := "CASCADE", "NO ACTION"
	rules := map[string]*string{"ord_c_fkey": &cascade, "ord_c_fkey1": &noAction}
	database := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "op", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "ord", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "c", DataType: "integer", IsNullable: "YES"},
			}},
		},
		Constraints: []catalog.Constraint{
			{TableName: "op", Name: "op_pkey", Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"}},
		},
	}
	for _, name := range names {
		database.Constraints = append(database.Constraints, catalog.Constraint{
			TableName: "ord", Name: name, Type: "FOREIGN KEY", ColumnName: "c", ColumnNames: []string{"c"},
			ForeignTable: new("op"), ForeignColumn: new("id"), ForeignColumns: []string{"id"},
			DeleteRule: rules[name], UpdateRule: &noAction,
		})
	}
	return database
}

// TestConvert_KeepsEveryForeignKeyOverOneColumn converts a table with two
// foreign keys over one column. The column carries the key whose name sorts
// first, whatever order the catalog reports them in, and the other stays a
// constraint of the table. Carried on the column alone, the second key was
// lost (stokaro/ptah#3873).
func TestConvert_KeepsEveryForeignKeyOverOneColumn(t *testing.T) {
	for _, order := range [][]string{{"ord_c_fkey", "ord_c_fkey1"}, {"ord_c_fkey1", "ord_c_fkey"}} {
		t.Run(order[0]+" first", func(t *testing.T) {
			c := qt.New(t)

			database := dbschematogo.ConvertDBSchemaToGoSchema(twoKeysOverOneColumn(order...), platform.Postgres)

			column := fieldNamed(c, database, "c")
			c.Assert(column.ForeignKeyName, qt.Equals, "ord_c_fkey")
			c.Assert(column.Foreign, qt.Equals, "op(id)")
			c.Assert(column.OnDelete, qt.Equals, "CASCADE")
			c.Assert(database.Constraints, qt.HasLen, 1)
			c.Assert(database.Constraints[0].Name, qt.Equals, "ord_c_fkey1")
			c.Assert(database.Constraints[0].Type, qt.Equals, "FOREIGN KEY")
			c.Assert(database.Constraints[0].Columns, qt.DeepEquals, []string{"c"})
			c.Assert(database.Constraints[0].ForeignTable, qt.Equals, "op")
			c.Assert(database.Constraints[0].ForeignColumn, qt.Equals, "id")
			c.Assert(database.Constraints[0].OnDelete, qt.Equals, "NO ACTION")
		})
	}
}

// TestConvert_LeavesALoneForeignKeyToItsColumn is the control: a column with
// one key carries it, and the table holds no constraint for it.
func TestConvert_LeavesALoneForeignKeyToItsColumn(t *testing.T) {
	c := qt.New(t)

	database := dbschematogo.ConvertDBSchemaToGoSchema(twoKeysOverOneColumn("ord_c_fkey1"), platform.Postgres)

	c.Assert(fieldNamed(c, database, "c").ForeignKeyName, qt.Equals, "ord_c_fkey1")
	c.Assert(database.Constraints, qt.HasLen, 0)
}

// TestCompare_ADatabaseWithTwoForeignKeysOverOneColumnIsSynced compares the
// description of a database with the database itself, the comparison
// `schema diff` runs between two databases: nothing is planned.
func TestCompare_ADatabaseWithTwoForeignKeysOverOneColumnIsSynced(t *testing.T) {
	c := qt.New(t)
	live := twoKeysOverOneColumn("ord_c_fkey", "ord_c_fkey1")

	diff := schemadiff.CompareWithDialect(
		dbschematogo.ConvertDBSchemaToGoSchema(live, platform.Postgres), live, platform.Postgres,
	)

	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("%+v", diff))
}

// TestCompare_ADatabaseMissingTheSecondForeignKeyIsPlannedIt is the control:
// against a database holding only the first key, the second is added.
func TestCompare_ADatabaseMissingTheSecondForeignKeyIsPlannedIt(t *testing.T) {
	c := qt.New(t)

	diff := schemadiff.CompareWithDialect(
		dbschematogo.ConvertDBSchemaToGoSchema(twoKeysOverOneColumn("ord_c_fkey", "ord_c_fkey1"), platform.Postgres),
		twoKeysOverOneColumn("ord_c_fkey"),
		platform.Postgres,
	)

	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
	c.Assert(diff.ConstraintsAdded.Names(), qt.DeepEquals, []string{"ord_c_fkey1"})
}
