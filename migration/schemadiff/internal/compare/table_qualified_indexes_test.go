package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

func TestIndexes_TableQualifiedAdditions(t *testing.T) {
	tests := []struct {
		name     string
		database *catalog.Database
		want     []difftypes.IndexRef
	}{
		{
			name:     "both indexes missing",
			database: &catalog.Database{},
			want: []difftypes.IndexRef{
				{Name: "idx_shared_lookup", TableName: "accounts"},
				{Name: "idx_shared_lookup", TableName: "users"},
			},
		},
		{
			name: "one table has the index",
			database: &catalog.Database{
				Indexes: []catalog.Index{
					{Name: "idx_shared_lookup", TableName: "accounts"},
				},
			},
			want: []difftypes.IndexRef{
				{Name: "idx_shared_lookup", TableName: "users"},
			},
		},
		{
			name: "both tables have the index",
			database: &catalog.Database{
				Indexes: []catalog.Index{
					{Name: "idx_shared_lookup", TableName: "accounts"},
					{Name: "idx_shared_lookup", TableName: "users"},
				},
			},
			want: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Indexes: []schemamodel.Index{
					{Name: "idx_shared_lookup", TableName: "users"},
					{Name: "idx_shared_lookup", TableName: "accounts"},
				},
			}
			diff := &difftypes.SchemaDiff{}

			compare.Indexes(desired, test.database, diff)

			c.Assert(diff.IndexAdditions(), qt.DeepEquals, test.want)
			c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
		})
	}
}

func TestIndexes_AddedTableIndexCarriesOwner(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "User", Name: "users"},
		},
		Indexes: []schemamodel.Index{
			{Name: "idx_users_email", StructName: "User", Fields: []string{"email"}},
		},
	}
	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableCreationsFor(desired, "users"),
	}

	compare.Indexes(desired, &catalog.Database{}, diff)

	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_users_email", TableName: "users"},
	})
}

func TestIndexes_TableQualifiedRemovals(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{}
	database := &catalog.Database{
		Indexes: []catalog.Index{
			{Name: "idx_shared_lookup", TableName: "users"},
			{Name: "idx_shared_lookup", TableName: "accounts"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.Indexes(desired, database, diff)

	c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_shared_lookup", TableName: "accounts"},
		{Name: "idx_shared_lookup", TableName: "users"},
	})
}

func TestIndexes_TableQualifiedReplacement(t *testing.T) {
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		database *catalog.Database
	}{
		{
			name: "predicate changed on one table",
			desired: &schemamodel.Database{
				Indexes: []schemamodel.Index{
					{Name: "idx_shared_lookup", TableName: "accounts", Condition: "deleted_at IS NULL"},
					{Name: "idx_shared_lookup", TableName: "users", Condition: "deleted_at IS NULL"},
				},
			},
			database: &catalog.Database{
				Indexes: []catalog.Index{
					{Name: "idx_shared_lookup", TableName: "accounts", Condition: "deleted_at IS NOT NULL"},
					{Name: "idx_shared_lookup", TableName: "users", Condition: "deleted_at IS NULL"},
				},
			},
		},
		{
			name: "nulls distinct changed on one table",
			desired: &schemamodel.Database{
				Indexes: []schemamodel.Index{
					{Name: "idx_shared_lookup", TableName: "accounts", Unique: true, NullsDistinct: new(false)},
					{Name: "idx_shared_lookup", TableName: "users", Unique: true, NullsDistinct: new(true)},
				},
			},
			database: &catalog.Database{
				Indexes: []catalog.Index{
					{Name: "idx_shared_lookup", TableName: "accounts", IsUnique: true, NullsDistinct: new(true)},
					{Name: "idx_shared_lookup", TableName: "users", IsUnique: true, NullsDistinct: new(true)},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.IndexesWithDialect(test.desired, test.database, diff, "postgres")

			want := []difftypes.IndexRef{
				{Name: "idx_shared_lookup", TableName: "accounts"},
			}
			c.Assert(diff.IndexAdditions(), qt.DeepEquals, want)
			c.Assert(diff.IndexRemovals(), qt.DeepEquals, want)
		})
	}
}

func TestIndexes_TableQualifiedRefsHaveDeterministicOrdering(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Indexes: []schemamodel.Index{
			{Name: "idx_shared_lookup", TableName: "zeta"},
			{Name: "z_idx", TableName: "alpha"},
			{Name: "a_idx", TableName: "alpha"},
		},
	}
	database := &catalog.Database{
		Indexes: []catalog.Index{
			{Name: "idx_shared_lookup", TableName: "omega"},
			{Name: "z_idx", TableName: "beta"},
			{Name: "a_idx", TableName: "beta"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.Indexes(desired, database, diff)

	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "a_idx", TableName: "alpha"},
		{Name: "z_idx", TableName: "alpha"},
		{Name: "idx_shared_lookup", TableName: "zeta"},
	})
	c.Assert(diff.IndexesRemoved, qt.DeepEquals, []difftypes.IndexRef{
		{Name: "a_idx", TableName: "beta"},
		{Name: "z_idx", TableName: "beta"},
		{Name: "idx_shared_lookup", TableName: "omega"},
	})
}

func TestIndexesWithDialect_CaseInsensitiveIdentityHasNoDiff(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		desired  schemamodel.Index
		database catalog.Index
	}{
		{
			name:     "mysql index name",
			dialect:  "mysql",
			desired:  schemamodel.Index{Name: "IDX_Users_Email", TableName: "Users"},
			database: catalog.Index{Name: "idx_users_email", TableName: "Users"},
		},
		{
			name:     "mariadb index name",
			dialect:  "mariadb",
			desired:  schemamodel.Index{Name: "IDX_Users_Email", TableName: "Users"},
			database: catalog.Index{Name: "idx_users_email", TableName: "Users"},
		},
		{
			name:    "sqlite schema table and index name",
			dialect: "sqlite",
			desired: schemamodel.Index{Name: "IDX_Users_Email", TableName: "Tenant.Users"},
			database: catalog.Index{
				Name:      "idx_users_email",
				Schema:    "tenant",
				TableName: "users",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Indexes: []schemamodel.Index{test.desired}}
			database := &catalog.Database{Indexes: []catalog.Index{test.database}}
			diff := &difftypes.SchemaDiff{}

			compare.IndexesWithDialect(desired, database, diff, test.dialect)

			c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
			c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
		})
	}
}

func TestIndexesWithDialect_CaseInsensitiveReplacementPreservesRawSpelling(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Indexes: []schemamodel.Index{
			{
				Name:      "IDX_Users_Active",
				TableName: "Tenant.Users",
				Condition: "deleted_at IS NULL",
			},
		},
	}
	database := &catalog.Database{
		Indexes: []catalog.Index{
			{
				Name:      "idx_users_active",
				Schema:    "tenant",
				TableName: "users",
				Condition: "deleted_at IS NOT NULL",
			},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.IndexesWithDialect(desired, database, diff, "sqlite")

	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "IDX_Users_Active", TableName: "Tenant.Users"},
	})
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_users_active", TableName: "tenant.users"},
	})
}
