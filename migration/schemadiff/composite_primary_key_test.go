package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff"
)

func TestCompareWithDialect_TableLevelCompositePrimaryKeyMatchesIntrospectedPostgresPrimaryKey(t *testing.T) {
	c := qt.New(t)
	generated := compositePrimaryKeySchema()
	database := &types.DBSchema{
		Tables: []types.DBTable{{
			Name: "memberships",
			Type: "TABLE",
			Columns: []types.DBColumn{
				{Name: "org_id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "user_id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "role", DataType: "text", IsNullable: "NO"},
			},
		}},
		Constraints: []types.DBConstraint{{
			Name:        "memberships_pkey",
			TableName:   "memberships",
			Type:        "PRIMARY KEY",
			ColumnNames: []string{"org_id", "user_id"},
		}},
	}

	diff := schemadiff.CompareWithDialect(generated, database, "postgres")
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}

func TestCompareWithDialect_TableLevelCompositePrimaryKeyMissingFromExistingTableIsAdded(t *testing.T) {
	c := qt.New(t)

	generated := compositePrimaryKeySchema()
	database := &types.DBSchema{
		Tables: []types.DBTable{{
			Name: "memberships",
			Type: "TABLE",
			Columns: []types.DBColumn{
				{Name: "org_id", DataType: "integer", IsNullable: "NO"},
				{Name: "user_id", DataType: "integer", IsNullable: "NO"},
				{Name: "role", DataType: "text", IsNullable: "NO"},
			},
		}},
	}

	diff := schemadiff.CompareWithDialect(generated, database, "postgres")
	c.Assert(diff.ConstraintsAdded, qt.DeepEquals, []string{"memberships_pkey"})
	c.Assert(diff.ConstraintsAddedWithTables, qt.HasLen, 1)
	c.Assert(diff.ConstraintsAddedWithTables[0].TableName, qt.Equals, "memberships")
	c.Assert(diff.ConstraintsAddedWithTables[0].Type, qt.Equals, "PRIMARY KEY")
	c.Assert(diff.ConstraintsAddedWithTables[0].Columns, qt.DeepEquals, []string{"org_id", "user_id"})
	c.Assert(diff.TablesModified, qt.HasLen, 0, qt.Commentf("diff: %#v", diff))
}

func TestCompareWithDialect_BlankTablePrimaryKeyDoesNotSynthesizeConstraint(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{{
			StructName: "User",
			Name:       "users",
			PrimaryKey: []string{""},
		}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true, Nullable: false},
			{StructName: "User", Name: "email", Type: "TEXT", Nullable: false},
		},
	}
	database := &types.DBSchema{
		Tables: []types.DBTable{{
			Name: "users",
			Type: "TABLE",
			Columns: []types.DBColumn{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "email", DataType: "text", IsNullable: "NO"},
			},
		}},
	}

	diff := schemadiff.CompareWithDialect(generated, database, "postgres")
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0, qt.Commentf("diff: %#v", diff))
	c.Assert(diff.ConstraintsAddedWithTables, qt.HasLen, 0, qt.Commentf("diff: %#v", diff))
}

func compositePrimaryKeySchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{
			StructName: "Membership",
			Name:       "memberships",
			PrimaryKey: []string{"org_id", "user_id"},
		}},
		Fields: []goschema.Field{
			{StructName: "Membership", Name: "org_id", Type: "INTEGER", Nullable: false},
			{StructName: "Membership", Name: "user_id", Type: "INTEGER", Nullable: false},
			{StructName: "Membership", Name: "role", Type: "TEXT", Nullable: false},
		},
	}
}
