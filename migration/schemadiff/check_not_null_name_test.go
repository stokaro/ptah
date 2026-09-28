package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
)

// TestCompareWithDatabaseInfo_ACheckNamedLikeANotNull compares a CHECK its
// author named `p_not_null` with the same CHECK in the database. The name is
// the one PostgreSQL gives a NOT NULL, and the row is still a CHECK: taken for
// the column's NOT NULL, it was left out of the comparison and every plan added
// it again (stokaro/ptah#3935).
func TestCompareWithDatabaseInfo_ACheckNamedLikeANotNull(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "C", Name: "c"}},
		Fields: []schemamodel.Field{
			{StructName: "C", Name: "id", Type: "integer", Primary: true},
			{StructName: "C", Name: "p", Type: "integer", Nullable: true},
		},
		Constraints: []schemamodel.Constraint{
			{StructName: "C", Table: "c", Name: "p_not_null", Type: "CHECK", CheckExpression: "p > 0"},
		},
	}
	database := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "c",
			Type: "BASE TABLE",
			Columns: []catalog.Column{
				{Name: "id", UDTName: "int4", IsPrimaryKey: true},
				{Name: "p", UDTName: "int4", IsNullable: "YES"},
			},
		}},
		Constraints: []catalog.Constraint{
			{TableName: "c", Name: "p_not_null", Type: "CHECK", CheckClause: new("(p > 0)")},
		},
	}

	diff, err := schemadiff.CompareWithDatabaseInfo(desired, database,
		catalog.ServerInfo{Dialect: "postgres", Schema: "public"}, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
}
