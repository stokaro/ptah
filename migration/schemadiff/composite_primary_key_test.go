package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
)

func TestCompareWithDialect_TableLevelCompositePrimaryKeyMatchesIntrospectedPostgresPrimaryKey(t *testing.T) {
	c := qt.New(t)
	desired := compositePrimaryKeySchema()
	database := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "memberships",
			Type: "TABLE",
			Columns: []catalog.Column{
				{Name: "org_id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "user_id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "role", DataType: "text", IsNullable: "NO"},
			},
		}},
		Constraints: []catalog.Constraint{{
			Name:        "memberships_pkey",
			TableName:   "memberships",
			Type:        "PRIMARY KEY",
			ColumnNames: []string{"org_id", "user_id"},
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, database, "postgres")
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}

func TestCompareWithDialect_TableLevelCompositePrimaryKeyMissingFromExistingTableIsAdded(t *testing.T) {
	c := qt.New(t)

	desired := compositePrimaryKeySchema()
	database := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "memberships",
			Type: "TABLE",
			Columns: []catalog.Column{
				{Name: "org_id", DataType: "integer", IsNullable: "NO"},
				{Name: "user_id", DataType: "integer", IsNullable: "NO"},
				{Name: "role", DataType: "text", IsNullable: "NO"},
			},
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, database, "postgres")
	c.Assert(diff.ConstraintsAdded.Names(), qt.DeepEquals, []string{"memberships_pkey"})
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 1)
	c.Assert(diff.ConstraintsAdded[0].TableName, qt.Equals, "memberships")
	c.Assert(diff.ConstraintsAdded[0].Type, qt.Equals, "PRIMARY KEY")
	c.Assert(diff.ConstraintsAdded[0].Columns, qt.DeepEquals, []string{"org_id", "user_id"})
	c.Assert(diff.TablesModified, qt.HasLen, 0, qt.Commentf("diff: %#v", diff))
}

func TestCompareWithDialect_BlankTablePrimaryKeyDoesNotSynthesizeConstraint(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "User",
			Name:       "users",
			PrimaryKey: []string{""},
		}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true, Nullable: false},
			{StructName: "User", Name: "email", Type: "TEXT", Nullable: false},
		},
	}
	database := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "users",
			Type: "TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "email", DataType: "text", IsNullable: "NO"},
			},
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, database, "postgres")
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0, qt.Commentf("diff: %#v", diff))
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0, qt.Commentf("diff: %#v", diff))
}

func TestCompareWithDialect_SingleColumnFieldLevelPrimaryKeyIsNotDuplicated(t *testing.T) {
	c := qt.New(t)

	// A single-column table-level PRIMARY KEY that is also carried as a column
	// primary key (Field.Primary) — the shape the SQL and HCL loaders produce —
	// must be compared field-to-field, not synthesized as a separate table
	// constraint. Otherwise an already-existing primary key is reported as a
	// spurious ADD PRIMARY KEY (#708).
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "Pet",
			Name:       "pets",
			PrimaryKey: []string{"id"},
		}},
		Fields: []schemamodel.Field{
			{StructName: "Pet", Name: "id", Type: "BIGINT", Primary: true, Nullable: false},
			{StructName: "Pet", Name: "name", Type: "TEXT", Nullable: false},
		},
	}
	database := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "pets",
			Type: "TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "name", DataType: "text", IsNullable: "NO"},
			},
		}},
		Constraints: []catalog.Constraint{{
			Name:        "pets_pkey",
			TableName:   "pets",
			Type:        "PRIMARY KEY",
			ColumnNames: []string{"id"},
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, database, "postgres")
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0, qt.Commentf("diff: %#v", diff))
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}

func TestCompareWithDialect_SingleColumnFieldLevelPrimaryKeyMissingFromDBIsDetected(t *testing.T) {
	c := qt.New(t)

	// The mirror of the previous test: when the database is missing the primary
	// key, comparison must still detect it (via the field-level path, since the
	// table-level constraint is not synthesized for a field-level PK). It must
	// not be silently dropped (#708).
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "Pet",
			Name:       "pets",
			PrimaryKey: []string{"id"},
		}},
		Fields: []schemamodel.Field{
			{StructName: "Pet", Name: "id", Type: "BIGINT", Primary: true, Nullable: false},
			{StructName: "Pet", Name: "name", Type: "TEXT", Nullable: false},
		},
	}
	database := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "pets",
			Type: "TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", IsNullable: "NO"},
				{Name: "name", DataType: "text", IsNullable: "NO"},
			},
		}},
	}

	diff := schemadiff.CompareWithDialect(desired, database, "postgres")
	c.Assert(diff.HasChanges(), qt.IsTrue, qt.Commentf("diff: %#v", diff))
	c.Assert(diff.ConstraintsAdded.Names(), qt.Contains, "pets_pkey", qt.Commentf("diff: %#v", diff))
}

func compositePrimaryKeySchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "Membership",
			Name:       "memberships",
			PrimaryKey: []string{"org_id", "user_id"},
		}},
		Fields: []schemamodel.Field{
			{StructName: "Membership", Name: "org_id", Type: "INTEGER", Nullable: false},
			{StructName: "Membership", Name: "user_id", Type: "INTEGER", Nullable: false},
			{StructName: "Membership", Name: "role", Type: "TEXT", Nullable: false},
		},
	}
}
