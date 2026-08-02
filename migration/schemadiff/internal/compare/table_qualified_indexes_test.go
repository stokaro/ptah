package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestIndexes_TableQualifiedAdditions(t *testing.T) {
	tests := []struct {
		name     string
		database *types.DBSchema
		want     []difftypes.IndexRef
	}{
		{
			name:     "both indexes missing",
			database: &types.DBSchema{},
			want: []difftypes.IndexRef{
				{Name: "idx_shared_lookup", TableName: "accounts"},
				{Name: "idx_shared_lookup", TableName: "users"},
			},
		},
		{
			name: "one table has the index",
			database: &types.DBSchema{
				Indexes: []types.DBIndex{
					{Name: "idx_shared_lookup", TableName: "accounts"},
				},
			},
			want: []difftypes.IndexRef{
				{Name: "idx_shared_lookup", TableName: "users"},
			},
		},
		{
			name: "both tables have the index",
			database: &types.DBSchema{
				Indexes: []types.DBIndex{
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
			generated := &goschema.Database{
				Indexes: []goschema.Index{
					{Name: "idx_shared_lookup", TableName: "users"},
					{Name: "idx_shared_lookup", TableName: "accounts"},
				},
			}
			diff := &difftypes.SchemaDiff{}

			compare.Indexes(generated, test.database, diff)

			c.Assert(diff.IndexAdditions(), qt.DeepEquals, test.want)
			c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
		})
	}
}

func TestIndexes_AddedTableIndexCarriesOwner(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Name: "users"},
		},
		Indexes: []goschema.Index{
			{Name: "idx_users_email", StructName: "User", Fields: []string{"email"}},
		},
	}
	diff := &difftypes.SchemaDiff{
		TablesAdded: []string{"users"},
	}

	compare.Indexes(generated, &types.DBSchema{}, diff)

	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_users_email", TableName: "users"},
	})
}

func TestIndexes_TableQualifiedRemovals(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{}
	database := &types.DBSchema{
		Indexes: []types.DBIndex{
			{Name: "idx_shared_lookup", TableName: "users"},
			{Name: "idx_shared_lookup", TableName: "accounts"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.Indexes(generated, database, diff)

	c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_shared_lookup", TableName: "accounts"},
		{Name: "idx_shared_lookup", TableName: "users"},
	})
}

func TestIndexes_TableQualifiedReplacement(t *testing.T) {
	tests := []struct {
		name      string
		generated *goschema.Database
		database  *types.DBSchema
	}{
		{
			name: "predicate changed on one table",
			generated: &goschema.Database{
				Indexes: []goschema.Index{
					{Name: "idx_shared_lookup", TableName: "accounts", Condition: "deleted_at IS NULL"},
					{Name: "idx_shared_lookup", TableName: "users", Condition: "deleted_at IS NULL"},
				},
			},
			database: &types.DBSchema{
				Indexes: []types.DBIndex{
					{Name: "idx_shared_lookup", TableName: "accounts", Condition: "deleted_at IS NOT NULL"},
					{Name: "idx_shared_lookup", TableName: "users", Condition: "deleted_at IS NULL"},
				},
			},
		},
		{
			name: "nulls distinct changed on one table",
			generated: &goschema.Database{
				Indexes: []goschema.Index{
					{Name: "idx_shared_lookup", TableName: "accounts", Unique: true, NullsDistinct: new(false)},
					{Name: "idx_shared_lookup", TableName: "users", Unique: true, NullsDistinct: new(true)},
				},
			},
			database: &types.DBSchema{
				Indexes: []types.DBIndex{
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

			compare.IndexesWithDialect(test.generated, test.database, diff, "postgres")

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
	generated := &goschema.Database{
		Indexes: []goschema.Index{
			{Name: "idx_shared_lookup", TableName: "zeta"},
			{Name: "z_idx", TableName: "alpha"},
			{Name: "a_idx", TableName: "alpha"},
		},
	}
	database := &types.DBSchema{
		Indexes: []types.DBIndex{
			{Name: "idx_shared_lookup", TableName: "omega"},
			{Name: "z_idx", TableName: "beta"},
			{Name: "a_idx", TableName: "beta"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.Indexes(generated, database, diff)

	c.Assert(diff.IndexesAdded, qt.DeepEquals, []difftypes.IndexRef{
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
	c := qt.New(t)
	tests := []struct {
		name      string
		dialect   string
		generated goschema.Index
		database  types.DBIndex
	}{
		{
			name:      "mysql index name",
			dialect:   "mysql",
			generated: goschema.Index{Name: "IDX_Users_Email", TableName: "Users"},
			database:  types.DBIndex{Name: "idx_users_email", TableName: "Users"},
		},
		{
			name:      "mariadb index name",
			dialect:   "mariadb",
			generated: goschema.Index{Name: "IDX_Users_Email", TableName: "Users"},
			database:  types.DBIndex{Name: "idx_users_email", TableName: "Users"},
		},
		{
			name:      "sqlite schema table and index name",
			dialect:   "sqlite",
			generated: goschema.Index{Name: "IDX_Users_Email", TableName: "Tenant.Users"},
			database: types.DBIndex{
				Name:      "idx_users_email",
				Schema:    "tenant",
				TableName: "users",
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			generated := &goschema.Database{Indexes: []goschema.Index{test.generated}}
			database := &types.DBSchema{Indexes: []types.DBIndex{test.database}}
			diff := &difftypes.SchemaDiff{}

			compare.IndexesWithDialect(generated, database, diff, test.dialect)

			c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
			c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
		})
	}
}

func TestIndexesWithDialect_CaseInsensitiveReplacementPreservesRawSpelling(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{
		Indexes: []goschema.Index{
			{
				Name:      "IDX_Users_Active",
				TableName: "Tenant.Users",
				Condition: "deleted_at IS NULL",
			},
		},
	}
	database := &types.DBSchema{
		Indexes: []types.DBIndex{
			{
				Name:      "idx_users_active",
				Schema:    "tenant",
				TableName: "users",
				Condition: "deleted_at IS NOT NULL",
			},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.IndexesWithDialect(generated, database, diff, "sqlite")

	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "IDX_Users_Active", TableName: "Tenant.Users"},
	})
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_users_active", TableName: "tenant.users"},
	})
}
