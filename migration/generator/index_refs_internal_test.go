package generator

// White-box testing required: table-qualified index identity is transformed by
// internal split, reverse, clone, rollback, and SQL rendering helpers whose
// exact intermediate results are not observable through the public API.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasmigrate"
	"ptah.run/migration/diffpolicy"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestSplitConcurrentIndexDiff_PreservesTableQualifiedIdentity(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	diff.SetIndexAdditions(difftypes.IndexChanges{
		{Index: schemamodel.Index{StructName: "Order", Name: "idx_shared", Fields: []string{"reference"}}, TableName: "users"},
		{Index: schemamodel.Index{StructName: "Order", Name: "idx_shared", Fields: []string{"reference"}}, TableName: "orders"},
	})
	got := splitConcurrentIndexDiff(diff, []difftypes.IndexRef{
		{Name: "idx_shared", TableName: "users"},
	}, nil)

	c.Assert(got.transactional.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_shared", TableName: "orders"},
	})
	c.Assert(got.noTransaction.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_shared", TableName: "users"},
	})
	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_shared", TableName: "orders"},
		{Name: "idx_shared", TableName: "users"},
	})
}

func TestConcurrentIndexRefsForPopulatedTables_SelectsExactIndex(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	diff.SetIndexAdditions(difftypes.IndexChanges{
		{Index: schemamodel.Index{StructName: "Order", Name: "idx_shared", Fields: []string{"reference"}}, TableName: "users"},
		{Index: schemamodel.Index{StructName: "Order", Name: "idx_shared", Fields: []string{"reference"}}, TableName: "orders"},
	})

	got := concurrentIndexRefsForPopulatedTables(
		diff,
		&catalog.Database{Tables: []catalog.Table{
			{Name: "users", EstimatedRows: 12},
			{Name: "orders", EstimatedRows: 0},
		}},
		catalog.ServerInfo{
			Dialect:      platform.Postgres,
			Capabilities: capability.Postgres16(),
		},
	)

	c.Assert(got, qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_shared", TableName: "users"},
	})
}

func TestPlanGeneratedMigrationSpecs_SkipDropIndexPreservesPostgresSchemaMove(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	diff.SetIndexAdditions(difftypes.IndexChanges{
		{Index: schemamodel.Index{StructName: "Order", Name: "idx_shared", Fields: []string{"reference"}}, TableName: "app.orders"},
	})
	diff.SetIndexRemovals([]difftypes.IndexRef{
		{Name: "idx_shared", TableName: "app.users"},
	})
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Order", Schema: "app", Name: "orders"},
		},
		Indexes: []schemamodel.Index{
			{StructName: "Order", Name: "idx_shared", Fields: []string{"reference"}},
		},
	}

	specs, _, err := planGeneratedMigrationSpecs(
		diff,
		desired,
		&catalog.Database{
			Tables: []catalog.Table{
				{Name: "users", Schema: "app", Type: "BASE TABLE"},
			},
			Indexes: []catalog.Index{
				{
					Name:      "idx_shared",
					TableName: "users",
					Schema:    "app",
					Columns:   []string{"legacy_reference"},
				},
			},
		},
		catalog.ServerInfo{
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
	diff := &difftypes.SchemaDiff{}
	diff.SetIndexAdditions(difftypes.IndexChanges{
		{Index: schemamodel.Index{StructName: "Order", Name: "idx_shared", Fields: []string{"reference"}}, TableName: "users"},
		{Index: schemamodel.Index{StructName: "Order", Name: "idx_shared", Fields: []string{"reference"}}, TableName: "orders"},
	})
	diff.SetIndexRemovals([]difftypes.IndexRef{
		{Name: "idx_legacy", TableName: "audit.events"},
	})

	got := reverseSchemaDiff(diff)

	c.Assert(got.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_legacy", TableName: "audit.events"},
	})
	c.Assert(got.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_shared", TableName: "orders"},
		{Name: "idx_shared", TableName: "users"},
	})
}

func TestCloneSchemaDiff_ClonesTableQualifiedIndexRepresentations(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	diff.SetIndexAdditions(difftypes.IndexChanges{
		{Index: schemamodel.Index{StructName: "Order", Name: "idx_shared", Fields: []string{"reference"}}, TableName: "users"},
	})

	got := cloneSchemaDiff(diff)
	got.IndexesAdded[0] = difftypes.IndexChange{Index: schemamodel.Index{Name: "idx_changed", Fields: []string{"email"}}, TableName: "orders"}

	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
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
	diff := &difftypes.SchemaDiff{
		IdentifierSemantics: &semantics,
		IndexesAdded:        difftypes.IndexChanges{{Index: schemamodel.Index{Name: "IDX_Email", Fields: []string{"email"}}, TableName: "dbo.users"}},
		IndexesRemoved: []difftypes.IndexRef{
			{Name: "idx_email", TableName: "dbo.users"},
		},
	}

	cloned := cloneSchemaDiff(diff)
	reversed := reverseSchemaDiffWithSchema(diff, nil, nil)
	split := splitConcurrentIndexDiff(diff, diff.IndexAdditions(), nil)

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
	diff := &difftypes.SchemaDiff{
		IndexesAdded: difftypes.IndexChanges{{Index: schemamodel.Index{Name: "idx_active_users", Fields: []string{"email"}}, TableName: "dbo.users"}},
		IndexesRemoved: []difftypes.IndexRef{
			{Name: "idx_active_users", TableName: "dbo.users"},
		},
	}
	desired := &schemamodel.Database{Indexes: []schemamodel.Index{
		{
			Name:      "idx_active_users",
			TableName: "dbo.users",
			Fields:    []string{"status"},
			Condition: "[status] = 2",
		},
	}}
	database := &catalog.Database{
		Tables: []catalog.Table{
			{
				Name:   "users",
				Schema: "dbo",
			},
		},
		Indexes: []catalog.Index{
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
		desired,
		database,
		platform.SQLServer,
		capability.SQLServer2022(),
	)

	c.Assert(err, qt.IsNil)
	// IF EXISTS is part of the emission on SQL Server: the guard is ACCEPTED on
	// every supported line, and a DOWN migration that drops without it fails
	// its second run (stokaro/ptah#916).
	c.Assert(sql, qt.Contains, "DROP INDEX IF EXISTS [idx_active_users] ON [dbo].[users]")
	c.Assert(sql, qt.Contains, "WHERE [status] = 1")
	c.Assert(
		strings.Index(sql, "DROP INDEX") < strings.Index(sql, "CREATE INDEX"),
		qt.IsTrue,
	)
}

func TestAddMySQLFamilyForeignKeyBackingIndexRemovals_PreservesDuplicateNames(t *testing.T) {
	c := qt.New(t)
	reverseDiff := &difftypes.SchemaDiff{}
	upDiff := &difftypes.SchemaDiff{
		ConstraintsAdded: []difftypes.ConstraintAdditionInfo{
			{Name: "fk_tenant", TableName: "orders", Type: "FOREIGN KEY"},
			{Name: "fk_tenant", TableName: "users", Type: "FOREIGN KEY"},
		},
	}

	err := addMySQLFamilyForeignKeyBackingIndexRemovals(
		reverseDiff,
		upDiff,
		&catalog.Database{},
		platform.MySQL,
		[]ast.Node{
			foreignKeyAdditionNode("orders", "fk_tenant", "tenant_id"),
			foreignKeyAdditionNode("users", "fk_tenant", "tenant_id"),
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(reverseDiff.IndexesRemoved, qt.DeepEquals, []difftypes.IndexRef{
		{Name: "fk_tenant", TableName: "orders"},
		{Name: "fk_tenant", TableName: "users"},
	})
	c.Assert(reverseDiff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "fk_tenant", TableName: "orders"},
		{Name: "fk_tenant", TableName: "users"},
	})
}

func TestAddMySQLFamilyForeignKeyBackingIndexRemovals_DoesNotCollideOnDots(t *testing.T) {
	c := qt.New(t)
	reverseDiff := &difftypes.SchemaDiff{}
	upDiff := &difftypes.SchemaDiff{
		ConstraintsAdded: []difftypes.ConstraintAdditionInfo{
			{Name: "c", TableName: "a.b", Type: "FOREIGN KEY"},
		},
	}
	live := &catalog.Database{
		Indexes: []catalog.Index{
			{Name: "b.c", TableName: "a"},
		},
	}

	err := addMySQLFamilyForeignKeyBackingIndexRemovals(
		reverseDiff,
		upDiff,
		live,
		platform.MySQL,
		[]ast.Node{foreignKeyAdditionNode("a.b", "c", "tenant_id")},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(reverseDiff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "c", TableName: "a.b"},
	})
}

func foreignKeyAdditionNode(table, name, column string) ast.Node {
	return &ast.AlterTableNode{
		Name: table,
		Operations: []ast.AlterOperation{&ast.AddConstraintOperation{
			Constraint: ast.NewForeignKeyConstraint(name, []string{column}, &ast.ForeignKeyRef{}),
		}},
	}
}
