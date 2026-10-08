package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// ordersDeclaring is a desired table orders whose key id is declared typ with
// a start and an increment, as the annotation parser builds it.
func ordersDeclaring(typ, start, increment string) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{{
			StructName: "Order", Name: "id", Type: typ, Primary: true, AutoInc: true,
			IdentityGeneration: "BY_DEFAULT", IdentityStart: start, IdentityIncrement: increment,
		}},
	}
	schemamodel.Finalize(db)
	return db
}

// ordersHolding is the catalog of table orders whose key id is a Serial column
// of ydbType whose sequence the reader reported this way.
func ordersHolding(ydbType string, column catalog.Column) *catalog.Database {
	column.Name, column.DataType, column.ColumnType = "id", ydbType, ydbType
	column.IsNullable, column.IsPrimaryKey, column.IsAutoIncrement = "NO", true, true
	return &catalog.Database{
		DatabasePath: "/local",
		Tables:       []catalog.Table{{Name: "orders", Type: "TABLE", Columns: []catalog.Column{column}}},
		Constraints: []catalog.Constraint{{Name: "orders_pkey", TableName: "orders", Type: "PRIMARY KEY",
			ColumnName: "id", ColumnNames: []string{"id"}}},
	}
}

// TestCompare_YDBSerialSequenceAgrees_HappyPath finds no difference where the
// declaration and the sequence agree, however each spells the settings: the
// reader leaves a setting of 1 empty, and a declaration may write it, or
// write `0100` for 100.
func TestCompare_YDBSerialSequenceAgrees_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name     string
		desired  *schemamodel.Database
		database *catalog.Database
	}{
		{name: "both at the defaults", desired: ordersDeclaring("BIGSERIAL", "", ""),
			database: ordersHolding("Int64", catalog.Column{})},
		{name: "the defaults written out", desired: ordersDeclaring("BIGSERIAL", "1", "1"),
			database: ordersHolding("Int64", catalog.Column{})},
		{name: "a start and an increment", desired: ordersDeclaring("BIGINT", "0100", "5"),
			database: ordersHolding("Int64", catalog.Column{IdentityStart: "100", IdentityIncrement: "5"})},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), test.desired, test.database, platform.YDB, must.Must(builtin.New())))
			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %+v", diff))
			c.Assert(diff.CurrentDatabasePath, qt.Equals, "/local")
		})
	}
}

// TestCompare_YDBSerialSequenceChanges records a changed start or increment
// under the attribute it is declared with, each on its own, and carries the
// value of a restart the database holds, which a plan refuses to replay.
func TestCompare_YDBSerialSequenceChanges(t *testing.T) {
	for _, test := range []struct {
		name        string
		desired     *schemamodel.Database
		database    *catalog.Database
		wantChanges map[string]string
		wantRestart string
	}{
		{
			name:        "a start declared on a sequence nobody altered",
			desired:     ordersDeclaring("BIGSERIAL", "100", ""),
			database:    ordersHolding("Int64", catalog.Column{}),
			wantChanges: map[string]string{"identity_start": "1 -> 100"},
		},
		{
			name:        "an increment changed",
			desired:     ordersDeclaring("BIGSERIAL", "100", "10"),
			database:    ordersHolding("Int64", catalog.Column{IdentityStart: "100", IdentityIncrement: "5"}),
			wantChanges: map[string]string{"identity_increment": "5 -> 10"},
		},
		{
			name:        "both moved back to the defaults on a restarted sequence",
			desired:     ordersDeclaring("BIGSERIAL", "", ""),
			database:    ordersHolding("Int64", catalog.Column{IdentityStart: "100", IdentityIncrement: "5", SequenceRestart: "100"}),
			wantChanges: map[string]string{"identity_start": "100 -> 1", "identity_increment": "5 -> 1"},
			wantRestart: "100",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), test.desired, test.database, platform.YDB, must.Must(builtin.New())))
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			column := diff.TablesModified[0].ColumnsModified[0]
			c.Assert(column.Changes, qt.DeepEquals, test.wantChanges)
			c.Assert(column.CurrentSequenceRestart, qt.Equals, test.wantRestart)
		})
	}
}

// TestCompare_SerialSequenceIsComparedOnlyWhereThePlannerChangesIt holds the
// other dialects to what they compare without this family: a PostgreSQL
// identity's start is not compared, because no other planner plans it, and on
// YDB a column that is a Serial on one side only is a type difference rather
// than a sequence one.
func TestCompare_SerialSequenceIsComparedOnlyWhereThePlannerChangesIt(t *testing.T) {
	for _, test := range []struct {
		name     string
		dialect  string
		desired  *schemamodel.Database
		database *catalog.Database
	}{
		{name: "a PostgreSQL identity", dialect: platform.Postgres, desired: ordersDeclaring("BIGINT", "100", "5"),
			database: ordersHolding("bigint", catalog.Column{IdentityGeneration: "BY_DEFAULT"})},
		{name: "a YDB column that is not a Serial in the database", dialect: platform.YDB,
			desired: ordersDeclaring("BIGSERIAL", "100", ""),
			database: func() *catalog.Database {
				db := ordersHolding("Int64", catalog.Column{})
				db.Tables[0].Columns[0].IsAutoIncrement = false
				return db
			}()},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), test.desired, test.database, test.dialect, must.Must(builtin.New())))
			for _, table := range diff.TablesModified {
				for _, column := range table.ColumnsModified {
					c.Assert(column.Changes["identity_start"], qt.Equals, "")
					c.Assert(column.Changes["identity_increment"], qt.Equals, "")
				}
			}
		})
	}
}

// TestCompareSchemas_YDBSerialSequenceSelfCompareReportsNothing pins the
// file-to-file path, where the current side is the same declaration turned
// into a catalog: the start and the increment have to cross that conversion,
// or a schema compared with itself plans an ALTER SEQUENCE.
func TestCompareSchemas_YDBSerialSequenceSelfCompareReportsNothing(t *testing.T) {
	c := qt.New(t)
	db := ordersDeclaring("BIGSERIAL", "100", "5")

	diff := must.Must(schemadiff.CompareSchemas(t.Context(), db, db, platform.YDB, must.Must(builtin.New())))

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %+v", diff))
	c.Assert(diff.TablesModified, qt.DeepEquals, []difftypes.TableDiff(nil))
}
