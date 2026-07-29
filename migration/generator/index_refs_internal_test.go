package generator

// White-box testing required: table-qualified index identity is transformed by
// internal split, reverse, clone, rollback, and SQL rendering helpers whose
// exact intermediate results are not observable through the public API.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/core/platform/identifier"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/atlasmigrate"
	"github.com/stokaro/ptah/migration/diffpolicy"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestSplitConcurrentIndexDiff_PreservesTableQualifiedIdentity(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{}
	diff.SetIndexAdditions([]types.IndexRef{
		{Name: "idx_shared", TableName: "users"},
		{Name: "idx_shared", TableName: "orders"},
	})
	got := splitConcurrentIndexDiff(diff, []types.IndexRef{
		{Name: "idx_shared", TableName: "users"},
	})

	c.Assert(got.transactional.IndexAdditions(), qt.DeepEquals, []types.IndexRef{
		{Name: "idx_shared", TableName: "orders"},
	})
	c.Assert(got.noTransaction.IndexAdditions(), qt.DeepEquals, []types.IndexRef{
		{Name: "idx_shared", TableName: "users"},
	})
	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []types.IndexRef{
		{Name: "idx_shared", TableName: "orders"},
		{Name: "idx_shared", TableName: "users"},
	})
}

func TestConcurrentIndexRefsForPopulatedTables_SelectsExactIndex(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{}
	diff.SetIndexAdditions([]types.IndexRef{
		{Name: "idx_shared", TableName: "users"},
		{Name: "idx_shared", TableName: "orders"},
	})

	got := concurrentIndexRefsForPopulatedTables(
		diff,
		&dbschematypes.DBSchema{Tables: []dbschematypes.DBTable{
			{Name: "users", EstimatedRows: 12},
			{Name: "orders", EstimatedRows: 0},
		}},
		dbschematypes.DBInfo{
			Dialect:      platform.Postgres,
			Capabilities: capability.Postgres16(),
		},
	)

	c.Assert(got, qt.DeepEquals, []types.IndexRef{
		{Name: "idx_shared", TableName: "users"},
	})
}

func TestPlanGeneratedMigrationSpecs_SkipDropIndexPreservesPostgresSchemaMove(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{}
	diff.SetIndexAdditions([]types.IndexRef{
		{Name: "idx_shared", TableName: "app.orders"},
	})
	diff.SetIndexRemovals([]types.IndexRef{
		{Name: "idx_shared", TableName: "app.users"},
	})
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Order", Schema: "app", Name: "orders"},
		},
		Indexes: []goschema.Index{
			{StructName: "Order", Name: "idx_shared", Fields: []string{"reference"}},
		},
	}

	specs, _, err := planGeneratedMigrationSpecs(
		diff,
		generated,
		&dbschematypes.DBSchema{
			Tables: []dbschematypes.DBTable{
				{Name: "users", Schema: "app", Type: "BASE TABLE"},
			},
			Indexes: []dbschematypes.DBIndex{
				{
					Name:      "idx_shared",
					TableName: "users",
					Schema:    "app",
					Columns:   []string{"legacy_reference"},
				},
			},
		},
		dbschematypes.DBInfo{
			Dialect:      platform.Postgres,
			Capabilities: capability.Postgres17(),
		},
		100,
		"move_index",
		DiffPolicy{SkipChangeKinds: []diffpolicy.ChangeKind{diffpolicy.DropIndex}},
		atlasmigrate.Qualifier{},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(specs, qt.HasLen, 1)
	c.Assert(specs[0].UpSQL, qt.Contains, `DROP INDEX IF EXISTS "app"."idx_shared"`)
	c.Assert(specs[0].UpSQL, qt.Contains, `CREATE INDEX IF NOT EXISTS "idx_shared" ON "app"."orders"`)
	c.Assert(
		strings.Index(specs[0].UpSQL, "DROP INDEX") < strings.Index(specs[0].UpSQL, "CREATE INDEX"),
		qt.IsTrue,
	)
}

func TestReverseSchemaDiff_PreservesTableQualifiedIndexIdentity(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{}
	diff.SetIndexAdditions([]types.IndexRef{
		{Name: "idx_shared", TableName: "users"},
		{Name: "idx_shared", TableName: "orders"},
	})
	diff.SetIndexRemovals([]types.IndexRef{
		{Name: "idx_legacy", TableName: "audit.events"},
	})

	got := reverseSchemaDiff(diff)

	c.Assert(got.IndexAdditions(), qt.DeepEquals, []types.IndexRef{
		{Name: "idx_legacy", TableName: "audit.events"},
	})
	c.Assert(got.IndexRemovals(), qt.DeepEquals, []types.IndexRef{
		{Name: "idx_shared", TableName: "orders"},
		{Name: "idx_shared", TableName: "users"},
	})
}

func TestCloneSchemaDiff_ClonesTableQualifiedIndexRepresentations(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{}
	diff.SetIndexAdditions([]types.IndexRef{
		{Name: "idx_shared", TableName: "users"},
	})

	got := cloneSchemaDiff(diff)
	got.IndexesAdded[0] = types.IndexRef{Name: "idx_changed", TableName: "orders"}

	c.Assert(diff.IndexesAdded, qt.DeepEquals, []types.IndexRef{
		{Name: "idx_shared", TableName: "users"},
	})
}

func TestIndexTransforms_PreserveIdentifierSemantics(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").
		WithResolvedNames([]identifier.ResolvedName{
			{Name: "dbo", Key: "dbo"},
			{Name: "users", Key: "users"},
			{Name: "IDX_Email", Key: "IDX_Email"},
			{Name: "idx_email", Key: "IDX_Email"},
		})
	diff := &types.SchemaDiff{
		IdentifierSemantics: &semantics,
		IndexesAdded: []types.IndexRef{
			{Name: "IDX_Email", TableName: "dbo.users"},
		},
		IndexesRemoved: []types.IndexRef{
			{Name: "idx_email", TableName: "dbo.users"},
		},
	}

	cloned := cloneSchemaDiff(diff)
	reversed := reverseSchemaDiffWithSchema(diff, nil, nil)
	split := splitConcurrentIndexDiff(diff, diff.IndexAdditions())

	c.Assert(cloned.IdentifierSemantics, qt.DeepEquals, &semantics)
	c.Assert(reversed.IdentifierSemantics, qt.DeepEquals, &semantics)
	c.Assert(split.transactional.IdentifierSemantics, qt.DeepEquals, &semantics)
	c.Assert(split.noTransaction.IdentifierSemantics, qt.DeepEquals, &semantics)

	cloned.IdentifierSemantics.ResolvedNames[0].Key = "cloned"
	c.Assert(semantics.ResolvedNames[0].Key, qt.Equals, "IDX_Email")
	c.Assert(reversed.IdentifierSemantics.ResolvedNames[0].Key, qt.Equals, "IDX_Email")
	c.Assert(split.transactional.IdentifierSemantics.ResolvedNames[0].Key, qt.Equals, "IDX_Email")
	c.Assert(split.noTransaction.IdentifierSemantics.ResolvedNames[0].Key, qt.Equals, "IDX_Email")

	reversed.IdentifierSemantics.ResolvedNames[0].Key = "reversed"
	c.Assert(semantics.ResolvedNames[0].Key, qt.Equals, "IDX_Email")
	c.Assert(split.transactional.IdentifierSemantics.ResolvedNames[0].Key, qt.Equals, "IDX_Email")
	c.Assert(split.noTransaction.IdentifierSemantics.ResolvedNames[0].Key, qt.Equals, "IDX_Email")

	split.transactional.IdentifierSemantics.ResolvedNames[0].Key = "transactional"
	c.Assert(semantics.ResolvedNames[0].Key, qt.Equals, "IDX_Email")
	c.Assert(split.noTransaction.IdentifierSemantics.ResolvedNames[0].Key, qt.Equals, "IDX_Email")
}

func TestGenerateDownMigrationSQL_SQLServerPreservesFilteredIndexPredicate(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "idx_active_users", TableName: "dbo.users"},
		},
		IndexesRemoved: []types.IndexRef{
			{Name: "idx_active_users", TableName: "dbo.users"},
		},
	}
	generated := &goschema.Database{Indexes: []goschema.Index{
		{
			Name:      "idx_active_users",
			TableName: "dbo.users",
			Fields:    []string{"status"},
			Condition: "[status] = 2",
		},
	}}
	database := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{
				Name:   "users",
				Schema: "dbo",
			},
		},
		Indexes: []dbschematypes.DBIndex{
			{
				Name:      "idx_active_users",
				TableName: "users",
				Schema:    "dbo",
				Columns:   []string{"status"},
				Condition: "[status] = 1",
			},
		},
	}

	sql, err := generateDownMigrationSQL(
		diff,
		generated,
		database,
		platform.SQLServer,
		capability.SQLServer2022(),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "DROP INDEX [idx_active_users] ON [dbo].[users]")
	c.Assert(sql, qt.Contains, "WHERE [status] = 1")
	c.Assert(
		strings.Index(sql, "DROP INDEX") < strings.Index(sql, "CREATE INDEX"),
		qt.IsTrue,
	)
}

func TestAddMySQLFamilyForeignKeyBackingIndexRemovals_PreservesDuplicateNames(t *testing.T) {
	c := qt.New(t)
	reverseDiff := &types.SchemaDiff{}
	upDiff := &types.SchemaDiff{
		ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{
			{Name: "fk_tenant", TableName: "orders", Type: "FOREIGN KEY"},
			{Name: "fk_tenant", TableName: "users", Type: "FOREIGN KEY"},
		},
	}

	addMySQLFamilyForeignKeyBackingIndexRemovals(
		reverseDiff,
		upDiff,
		&dbschematypes.DBSchema{},
		platform.MySQL,
	)

	c.Assert(reverseDiff.IndexesRemoved, qt.DeepEquals, []types.IndexRef{
		{Name: "fk_tenant", TableName: "orders"},
		{Name: "fk_tenant", TableName: "users"},
	})
	c.Assert(reverseDiff.IndexRemovals(), qt.DeepEquals, []types.IndexRef{
		{Name: "fk_tenant", TableName: "orders"},
		{Name: "fk_tenant", TableName: "users"},
	})
}

func TestAddMySQLFamilyForeignKeyBackingIndexRemovals_DoesNotCollideOnDots(t *testing.T) {
	c := qt.New(t)
	reverseDiff := &types.SchemaDiff{}
	upDiff := &types.SchemaDiff{
		ConstraintsAddedWithTables: []types.ConstraintAdditionInfo{
			{Name: "c", TableName: "a.b", Type: "FOREIGN KEY"},
		},
	}
	live := &dbschematypes.DBSchema{
		Indexes: []dbschematypes.DBIndex{
			{Name: "b.c", TableName: "a"},
		},
	}

	addMySQLFamilyForeignKeyBackingIndexRemovals(
		reverseDiff,
		upDiff,
		live,
		platform.MySQL,
	)

	c.Assert(reverseDiff.IndexRemovals(), qt.DeepEquals, []types.IndexRef{
		{Name: "c", TableName: "a.b"},
	})
}

func TestCollectShadowMismatches_ReportsQualifiedIndex(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{}
	diff.SetIndexAdditions([]types.IndexRef{
		{Name: "idx_shared", TableName: "users"},
		{Name: "idx_shared", TableName: "orders"},
	})

	got := collectShadowMismatches(diff)

	c.Assert(got, qt.DeepEquals, []ShadowMismatch{
		{
			Kind:    "missing_index",
			Table:   "orders",
			Object:  "orders.idx_shared",
			Message: "missing index orders.idx_shared",
		},
	})
}
