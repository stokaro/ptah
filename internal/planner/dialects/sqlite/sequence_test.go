package sqlite_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// keepSequence is the statement that gives the rebuilt users the high-water
// mark the live one holds, and the copy that follows it.
const keepSequence = "INSERT INTO sqlite_sequence (name, seq) SELECT 'new_users', seq FROM sqlite_sequence WHERE name = 'users';\n" +
	"INSERT INTO \"new_users\" ("

// usersWithSerial declares users(id, email) with id an AUTOINCREMENT key when
// serial is set.
func usersWithSerial(serial bool) *schemamodel.Database {
	desired := carriedUsers(false)
	desired.Fields[0].AutoInc = serial
	return desired
}

// usersRebuildFrom is the rebuild that drops column name from users, over a
// live id column that has AUTOINCREMENT when serial is set.
func usersRebuildFrom(serial bool) *difftypes.SchemaDiff {
	diff := usersRebuildCarrying(catalog.TableParts{})
	diff.TablesModified[0].Current.Table.Columns[0].IsAutoIncrement = serial
	return diff
}

// The copy leaves the rebuilt table's AUTOINCREMENT mark at the largest key it
// copies, and the drop deletes the old mark, so a key handed out and then
// deleted would be handed out again. Where both tables have AUTOINCREMENT, the
// plan gives the new table the old mark right after it creates the table,
// which makes sure sqlite_sequence exists, and before the copy.
func TestPlannerRebuildKeepsTheAutoincrementMark(t *testing.T) {
	c := qt.New(t)

	sql, err := planner.GenerateSchemaDiffSQL(context.Background(), must.Must(builtin.New()),
		withDeclaredTable(usersRebuildFrom(true), usersWithSerial(true)), platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "\n);\n-- Keep the AUTOINCREMENT high-water mark of users through the rebuild\n"+keepSequence)
}

// A key an exclude selector keeps out of scope is carried into the rebuilt
// table with its AUTOINCREMENT, and its mark is kept like a declared key's.
func TestPlannerRebuildKeepsTheMarkOfAKeyOutOfScope(t *testing.T) {
	c := qt.New(t)
	diff := usersRebuildCarrying(catalog.TableParts{
		Tables: carriedTable(autoIncrement(keyColumn(integerColumn(carriedColumn("serial"))))),
		Constraints: []catalog.Constraint{{
			TableName: "users", Name: "users_pkey", Type: "PRIMARY KEY", ColumnNames: []string{"serial"},
		}},
	})

	sql, err := planner.GenerateSchemaDiffSQL(context.Background(), must.Must(builtin.New()),
		withDeclaredTable(diff, carriedUsers(true)), platform.SQLite)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, keepSequence)
}

// A mark is kept only where it means something on both sides: the live table
// must have AUTOINCREMENT, so sqlite_sequence exists to be read, and the
// rebuilt one must have it too, or the rename would give a table without it a
// mark nothing reads.
func TestPlannerRebuildWritesNoMarkWithoutAutoincrementOnBothSides(t *testing.T) {
	tests := []struct {
		name           string
		live, declared bool
	}{
		{name: "the live table has none", declared: true},
		{name: "the declaration drops it", live: true},
		{name: "neither has it"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := planner.GenerateSchemaDiffSQL(context.Background(), must.Must(builtin.New()),
				withDeclaredTable(usersRebuildFrom(test.live), usersWithSerial(test.declared)), platform.SQLite)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, "CREATE TABLE \"new_users\" (")
			c.Assert(sql, qt.Not(qt.Contains), "sqlite_sequence")
		})
	}
}
