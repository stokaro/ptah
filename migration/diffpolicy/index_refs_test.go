package diffpolicy_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/diffpolicy"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func TestApplyDropIndex_PreservesOnlyExactReplacement(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	diff.SetIndexAdditions(difftypes.IndexChanges{
		{Index: schemamodel.Index{Name: "idx_shared", Fields: []string{"code"}}, TableName: "users"},
	})
	diff.SetIndexRemovals([]difftypes.IndexRef{
		{Name: "idx_shared", TableName: "users"},
		{Name: "idx_shared", TableName: "orders"},
	})

	got, skipped := diffpolicy.Apply(diff, diffpolicy.NewSkipSet(diffpolicy.DropIndex))

	c.Assert(got.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_shared", TableName: "users"},
	})
	c.Assert(skipped, qt.DeepEquals, []diffpolicy.SkippedChange{
		{Kind: diffpolicy.DropIndex, Object: "orders.idx_shared"},
	})
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_shared", TableName: "orders"},
		{Name: "idx_shared", TableName: "users"},
	})
}

func TestApplyForDialectDropIndex_PreservesPostgresSchemaReplacement(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	diff.SetIndexAdditions(difftypes.IndexChanges{
		{Index: schemamodel.Index{Name: "idx_shared", Fields: []string{"code"}}, TableName: "app.orders"},
	})
	diff.SetIndexRemovals([]difftypes.IndexRef{
		{Name: "idx_shared", TableName: "app.users"},
	})

	got, skipped := diffpolicy.ApplyForDialect(
		diff,
		diffpolicy.NewSkipSet(diffpolicy.DropIndex),
		"postgres",
	)

	c.Assert(got.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_shared", TableName: "app.users"},
	})
	c.Assert(skipped, qt.HasLen, 0)
}

func TestApplyForDialectDropIndex_SkipsMySQLDifferentTableRemoval(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	diff.SetIndexAdditions(difftypes.IndexChanges{
		{Index: schemamodel.Index{Name: "idx_shared", Fields: []string{"code"}}, TableName: "orders"},
	})
	diff.SetIndexRemovals([]difftypes.IndexRef{
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
	tests := []struct {
		name     string
		dialect  string
		addition difftypes.IndexRef
		removal  difftypes.IndexRef
	}{
		{
			name:     "mysql",
			dialect:  "mysql",
			addition: difftypes.IndexRef{Name: "IDX_Shared", TableName: "users"},
			removal:  difftypes.IndexRef{Name: "idx_shared", TableName: "users"},
		},
		{
			name:     "mariadb",
			dialect:  "mariadb",
			addition: difftypes.IndexRef{Name: "IDX_Shared", TableName: "users"},
			removal:  difftypes.IndexRef{Name: "idx_shared", TableName: "users"},
		},
		{
			name:     "sqlite",
			dialect:  "sqlite",
			addition: difftypes.IndexRef{Name: "IDX_Shared", TableName: "Tenant.Orders"},
			removal:  difftypes.IndexRef{Name: "idx_shared", TableName: "tenant.users"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{
				IndexesAdded:   difftypes.IndexChangesFromRefs(test.addition),
				IndexesRemoved: []difftypes.IndexRef{test.removal},
			}

			got, skipped := diffpolicy.ApplyForDialect(
				diff,
				diffpolicy.NewSkipSet(diffpolicy.DropIndex),
				test.dialect,
			)

			c.Assert(got.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{test.removal})
			c.Assert(skipped, qt.HasLen, 0)
		})
	}
}

func TestApplyForDialectDropIndex_PreservesPotentialSQLServerReplacement(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		IndexesAdded: difftypes.IndexChanges{
			{Index: schemamodel.Index{Name: "IDX_Shared", Fields: []string{"email"}}, TableName: "users"},
		},
		IndexesRemoved: []difftypes.IndexRef{
			{Name: "idx_shared", TableName: "users"},
		},
	}

	got, skipped := diffpolicy.ApplyForDialect(
		diff,
		diffpolicy.NewSkipSet(diffpolicy.DropIndex),
		"sqlserver",
	)

	c.Assert(got.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_shared", TableName: "users"},
	})
	c.Assert(skipped, qt.HasLen, 0)
}

func TestApplyForDialectDropIndex_SQLServerCaseSensitiveSkipsIndependentRemoval(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CS_AS").
		WithResolvedNames([]identifier.ResolvedName{
			{Name: "dbo", Key: "dbo"},
			{Name: "users", Key: "users"},
			{Name: "IDX_Shared", Key: "IDX_Shared"},
			{Name: "idx_shared", Key: "idx_shared"},
		})
	diff := &difftypes.SchemaDiff{
		IdentifierSemantics: &semantics,
		IndexesAdded: difftypes.IndexChanges{
			{Index: schemamodel.Index{Name: "IDX_Shared", Fields: []string{"email"}}, TableName: "dbo.users"},
		},
		IndexesRemoved: []difftypes.IndexRef{
			{Name: "idx_shared", TableName: "dbo.users"},
		},
	}

	got, skipped := diffpolicy.ApplyForDialect(
		diff,
		diffpolicy.NewSkipSet(diffpolicy.DropIndex),
		"sqlserver",
	)

	c.Assert(got.IndexRemovals(), qt.HasLen, 0)
	c.Assert(skipped, qt.DeepEquals, []diffpolicy.SkippedChange{
		{Kind: diffpolicy.DropIndex, Object: "dbo.users.idx_shared"},
	})
}

func TestApplyDropTable_PreservesSameNamedIndexOnKeptTable(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{TablesRemoved: []string{"users"}}
	diff.SetIndexRemovals([]difftypes.IndexRef{
		{Name: "idx_shared", TableName: "users"},
		{Name: "idx_shared", TableName: "orders"},
	})

	got, skipped := diffpolicy.Apply(diff, diffpolicy.NewSkipSet(diffpolicy.DropTable))

	c.Assert(got.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_shared", TableName: "orders"},
	})
	c.Assert(skipped, qt.DeepEquals, []diffpolicy.SkippedChange{
		{Kind: diffpolicy.DropTable, Object: "users"},
	})
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_shared", TableName: "orders"},
		{Name: "idx_shared", TableName: "users"},
	})
}
