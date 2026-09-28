package schemadiff_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// methodDesired is table t with a primary key over id asking for method.
func methodDesired(method string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "T", Name: "t", PrimaryKey: []string{"id"}, PrimaryKeyMethod: method,
		}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "INT", Primary: true},
		},
	}
}

// methodCurrent is table t as the MySQL-family reader reports it, the primary
// key carrying the method the server reports for its PRIMARY index, nil for
// BTREE.
func methodCurrent(method *string) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "t", Columns: []catalog.Column{
			{Name: "id", DataType: "int", ColumnType: "int", IsNullable: "NO", IsPrimaryKey: true},
		}}},
		Constraints: []catalog.Constraint{{
			Name: "PRIMARY", TableName: "t", Type: "PRIMARY KEY", ColumnNames: []string{"id"}, ColumnName: "id",
			UsingMethod: method,
		}},
	}
}

// TestCompare_PrimaryKeyMethod_Synced plans nothing where the key the server
// reports answers the method declared: the same method, a key declaring none,
// and on MySQL a HASH key the server built as BTREE, as MySQL 8.4.11 does on
// InnoDB (stokaro/ptah#3853).
func TestCompare_PrimaryKeyMethod_Synced(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		desired  string
		database *string
	}{
		{name: "HASH on MariaDB", dialect: platform.MariaDB, desired: "HASH", database: new("HASH")},
		{name: "no method on MariaDB", dialect: platform.MariaDB},
		{name: "no method beside a HASH key", dialect: platform.MariaDB, database: new("HASH")},
		{name: "HASH built as BTREE on MySQL", dialect: platform.MySQL, desired: "HASH"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(methodDesired(test.desired), methodCurrent(test.database), test.dialect)

			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
		})
	}
}

// TestCompare_PrimaryKeyMethod_Changed plans a MariaDB key asking for HASH over
// a BTREE key as a drop and an add of the key, the add carrying the method.
func TestCompare_PrimaryKeyMethod_Changed(t *testing.T) {
	c := qt.New(t)
	diff := schemadiff.CompareWithDialect(methodDesired("HASH"), methodCurrent(nil), platform.MariaDB)

	plan, err := planner.GenerateSchemaDiffSQLStatements(diff, platform.MariaDB)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(plan, "\n"), qt.Contains, "DROP PRIMARY KEY")
	c.Assert(strings.Join(plan, "\n"), qt.Contains, "PRIMARY KEY (`id`) USING HASH")
}
