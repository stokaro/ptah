package diffpolicy_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/diffpolicy"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestApplyDropIndex_PreservesOnlyExactReplacement(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{}
	diff.SetIndexAdditions([]types.IndexRef{
		{Name: "idx_shared", TableName: "users"},
	})
	diff.SetIndexRemovals([]types.IndexRef{
		{Name: "idx_shared", TableName: "users"},
		{Name: "idx_shared", TableName: "orders"},
	})

	got, skipped := diffpolicy.Apply(diff, diffpolicy.NewSkipSet(diffpolicy.DropIndex))

	c.Assert(got.IndexRemovals(), qt.DeepEquals, []types.IndexRef{
		{Name: "idx_shared", TableName: "users"},
	})
	c.Assert(skipped, qt.DeepEquals, []diffpolicy.SkippedChange{
		{Kind: diffpolicy.DropIndex, Object: "orders.idx_shared"},
	})
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []types.IndexRef{
		{Name: "idx_shared", TableName: "orders"},
		{Name: "idx_shared", TableName: "users"},
	})
}

func TestApplyForDialectDropIndex_PreservesPostgresSchemaReplacement(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{}
	diff.SetIndexAdditions([]types.IndexRef{
		{Name: "idx_shared", TableName: "app.orders"},
	})
	diff.SetIndexRemovals([]types.IndexRef{
		{Name: "idx_shared", TableName: "app.users"},
	})

	got, skipped := diffpolicy.ApplyForDialect(
		diff,
		diffpolicy.NewSkipSet(diffpolicy.DropIndex),
		"postgres",
	)

	c.Assert(got.IndexRemovals(), qt.DeepEquals, []types.IndexRef{
		{Name: "idx_shared", TableName: "app.users"},
	})
	c.Assert(skipped, qt.HasLen, 0)
}

func TestApplyForDialectDropIndex_SkipsMySQLDifferentTableRemoval(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{}
	diff.SetIndexAdditions([]types.IndexRef{
		{Name: "idx_shared", TableName: "orders"},
	})
	diff.SetIndexRemovals([]types.IndexRef{
		{Name: "idx_shared", TableName: "users"},
	})

	got, skipped := diffpolicy.ApplyForDialect(
		diff,
		diffpolicy.NewSkipSet(diffpolicy.DropIndex),
		"mysql",
	)

	c.Assert(got.IndexRemovals(), qt.HasLen, 0)
	c.Assert(skipped, qt.DeepEquals, []diffpolicy.SkippedChange{
		{Kind: diffpolicy.DropIndex, Object: "users.idx_shared"},
	})
}

func TestApplyForDialectDropIndex_PreservesCaseInsensitiveReplacement(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name     string
		dialect  string
		addition types.IndexRef
		removal  types.IndexRef
	}{
		{
			name:     "mysql",
			dialect:  "mysql",
			addition: types.IndexRef{Name: "IDX_Shared", TableName: "users"},
			removal:  types.IndexRef{Name: "idx_shared", TableName: "users"},
		},
		{
			name:     "mariadb",
			dialect:  "mariadb",
			addition: types.IndexRef{Name: "IDX_Shared", TableName: "users"},
			removal:  types.IndexRef{Name: "idx_shared", TableName: "users"},
		},
		{
			name:     "sqlite",
			dialect:  "sqlite",
			addition: types.IndexRef{Name: "IDX_Shared", TableName: "Tenant.Orders"},
			removal:  types.IndexRef{Name: "idx_shared", TableName: "tenant.users"},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			diff := &types.SchemaDiff{
				IndexesAdded:   []types.IndexRef{test.addition},
				IndexesRemoved: []types.IndexRef{test.removal},
			}

			got, skipped := diffpolicy.ApplyForDialect(
				diff,
				diffpolicy.NewSkipSet(diffpolicy.DropIndex),
				test.dialect,
			)

			c.Assert(got.IndexRemovals(), qt.DeepEquals, []types.IndexRef{test.removal})
			c.Assert(skipped, qt.HasLen, 0)
		})
	}
}

func TestApplyForDialectDropIndex_DoesNotAssumeSQLServerCollation(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "IDX_Shared", TableName: "users"},
		},
		IndexesRemoved: []types.IndexRef{
			{Name: "idx_shared", TableName: "users"},
		},
	}

	got, skipped := diffpolicy.ApplyForDialect(
		diff,
		diffpolicy.NewSkipSet(diffpolicy.DropIndex),
		"sqlserver",
	)

	c.Assert(got.IndexRemovals(), qt.HasLen, 0)
	c.Assert(skipped, qt.DeepEquals, []diffpolicy.SkippedChange{
		{Kind: diffpolicy.DropIndex, Object: "users.idx_shared"},
	})
}

func TestApplyDropTable_PreservesSameNamedIndexOnKeptTable(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{TablesRemoved: []string{"users"}}
	diff.SetIndexRemovals([]types.IndexRef{
		{Name: "idx_shared", TableName: "users"},
		{Name: "idx_shared", TableName: "orders"},
	})

	got, skipped := diffpolicy.Apply(diff, diffpolicy.NewSkipSet(diffpolicy.DropTable))

	c.Assert(got.IndexRemovals(), qt.DeepEquals, []types.IndexRef{
		{Name: "idx_shared", TableName: "orders"},
	})
	c.Assert(skipped, qt.DeepEquals, []diffpolicy.SkippedChange{
		{Kind: diffpolicy.DropTable, Object: "users"},
	})
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []types.IndexRef{
		{Name: "idx_shared", TableName: "orders"},
		{Name: "idx_shared", TableName: "users"},
	})
}
