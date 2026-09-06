package postgres_test

import (
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestPlanner_IndexRefs_QualifiesDropsAndReplacesOnlyExactRef(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		IndexesAdded: difftypes.IndexChanges{
			{Index: schemamodel.Index{Name: "idx_shared", StructName: "AppRecord", Fields: []string{"external_id"}}, TableName: "app.records"},
			{Index: schemamodel.Index{Name: "idx_shared", StructName: "LogRecord", Fields: []string{"recorded_at"}}, TableName: "logs.records"},
		},
		IndexesRemoved: []difftypes.IndexRef{
			{Name: "idx_shared", TableName: "app.records"},
			{Name: "idx_shared", TableName: "archive.records"},
		},
	}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "records", Schema: "app", StructName: "AppRecord"},
			{Name: "records", Schema: "logs", StructName: "LogRecord"},
		},
		Indexes: []schemamodel.Index{
			{Name: "idx_shared", StructName: "LogRecord", Fields: []string{"recorded_at"}},
			{Name: "idx_shared", StructName: "AppRecord", Fields: []string{"external_id"}},
		},
	}

	nodes, err := postgres.New().GenerateMigrationAST(withDeclaredObjects(diff, desired))
	c.Assert(err, qt.IsNil)

	c.Assert(nodes, qt.HasLen, 4)
	replacementDrop, ok := nodes[0].(*ast.DropIndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(replacementDrop.Name, qt.Equals, "idx_shared")
	c.Assert(replacementDrop.IfExists, qt.IsTrue)
	appCreate, ok := nodes[1].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(appCreate.Name, qt.Equals, "idx_shared")
	c.Assert(appCreate.Table, qt.Equals, "app.records")
	c.Assert(appCreate.Columns, qt.DeepEquals, []string{"external_id"})
	logsCreate, ok := nodes[2].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(logsCreate.Name, qt.Equals, "idx_shared")
	c.Assert(logsCreate.Table, qt.Equals, "logs.records")
	c.Assert(logsCreate.Columns, qt.DeepEquals, []string{"recorded_at"})
	archiveDrop, ok := nodes[3].(*ast.DropIndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(archiveDrop.Name, qt.Equals, "idx_shared")
	c.Assert(archiveDrop.IfExists, qt.IsTrue)
}

func TestPlanner_IndexRefs_DropsSameSchemaNameBeforeMovingIndex(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{Name: "orders", Schema: "app", StructName: "Order"},
		},
		Indexes: []schemamodel.Index{
			{Name: "idx_shared", StructName: "Order", Fields: []string{"reference"}},
		},
	}
	diff := &difftypes.SchemaDiff{
		IndexesAdded: difftypes.IndexAdditionsFor(desired, difftypes.IndexRef{Name: "idx_shared", TableName: "app.orders"}),
		IndexesRemoved: []difftypes.IndexRef{
			{Name: "idx_shared", TableName: "app.users"},
		},
	}

	nodes, err := postgres.New().GenerateMigrationAST(withDeclaredObjects(diff, desired))
	c.Assert(err, qt.IsNil)

	c.Assert(nodes, qt.HasLen, 2)
	drop, ok := nodes[0].(*ast.DropIndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(drop.Name, qt.Equals, "idx_shared")
	create, ok := nodes[1].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(create.Table, qt.Equals, "app.orders")
}

func TestPlanner_IndexRefs_CockroachDBPreservesReplacementTable(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	diff.SetIndexAdditions(difftypes.IndexChanges{
		{Index: schemamodel.Index{Name: "idx_shared", StructName: "LogRecord", Fields: []string{"recorded_at"}}, TableName: "public.users"},
	})
	diff.SetIndexRemovals([]difftypes.IndexRef{
		{Name: "idx_shared", TableName: "public.users"},
	})
	desired := &schemamodel.Database{
		Indexes: []schemamodel.Index{
			{Name: "idx_shared", TableName: "public.users", Fields: []string{"handle"}},
		},
	}

	nodes, err := postgres.NewForDialect(platform.CockroachDB, capability.CockroachDB23()).
		GenerateMigrationAST(withDeclaredObjects(diff, desired))
	c.Assert(err, qt.IsNil)

	c.Assert(nodes, qt.HasLen, 2)
	drop, ok := nodes[0].(*ast.DropIndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(drop.Name, qt.Equals, "idx_shared")
	c.Assert(drop.Table, qt.Equals, "public.users")
	create, ok := nodes[1].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(create.Table, qt.Equals, "public.users")
}

func TestPlanner_IndexRefs_SpannerDropsSameSchemaNameBeforeMovingIndex(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	diff.SetIndexAdditions(difftypes.IndexChanges{
		{Index: schemamodel.Index{Name: "idx_shared", StructName: "LogRecord", Fields: []string{"recorded_at"}}, TableName: "app.orders"},
	})
	diff.SetIndexRemovals([]difftypes.IndexRef{
		{Name: "idx_shared", TableName: "app.users"},
	})
	desired := &schemamodel.Database{
		Indexes: []schemamodel.Index{
			{Name: "idx_shared", TableName: "app.orders", Fields: []string{"reference"}},
		},
	}

	nodes, err := postgres.NewForDialect(platform.Spanner, capability.SpannerPostgres()).
		GenerateMigrationAST(withDeclaredObjects(diff, desired))
	c.Assert(err, qt.IsNil)

	c.Assert(nodes, qt.HasLen, 2)
	drop, ok := nodes[0].(*ast.DropIndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(drop.Name, qt.Equals, "idx_shared")
	create, ok := nodes[1].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(create.Table, qt.Equals, "app.orders")
}

func TestPlanner_IndexRefs_SpannerKeepsDifferentSchemaIndexesIndependent(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	diff.SetIndexAdditions(difftypes.IndexChanges{
		{Index: schemamodel.Index{Name: "idx_shared", StructName: "LogRecord", Fields: []string{"recorded_at"}}, TableName: "app.orders"},
	})
	diff.SetIndexRemovals([]difftypes.IndexRef{
		{Name: "idx_shared", TableName: "logs.users"},
	})
	desired := &schemamodel.Database{
		Indexes: []schemamodel.Index{
			{Name: "idx_shared", TableName: "app.orders", Fields: []string{"reference"}},
		},
	}

	nodes, err := postgres.NewForDialect(platform.Spanner, capability.SpannerPostgres()).
		GenerateMigrationAST(withDeclaredObjects(diff, desired))
	c.Assert(err, qt.IsNil)

	c.Assert(nodes, qt.HasLen, 2)
	create, ok := nodes[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(create.Table, qt.Equals, "app.orders")
	drop, ok := nodes[1].(*ast.DropIndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(drop.Name, qt.Equals, "idx_shared")
}

func TestPlanner_IndexRefs_UsesCanonicalOwnerAcrossPostgresFamily(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
	}{
		{name: "postgres", dialect: platform.Postgres, caps: capability.Postgres16()},
		{name: "cockroachdb", dialect: platform.CockroachDB, caps: capability.CockroachDB23()},
		{name: "yugabytedb", dialect: platform.YugabyteDB, caps: capability.YugabyteDB25()},
		{name: "spanner", dialect: platform.Spanner, caps: capability.SpannerPostgres()},
	}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Shared", Schema: "app", Name: "users"},
			{StructName: "Shared", Schema: "archive", Name: "records"},
		},
		Indexes: []schemamodel.Index{
			{
				Name:       "idx_users_email",
				StructName: "Shared",
				TableName:  "app.users",
				Fields:     []string{"email"},
			},
		},
	}
	diff := &difftypes.SchemaDiff{
		IndexesAdded: difftypes.IndexAdditionsFor(desired, difftypes.IndexRef{Name: "idx_users_email", TableName: "app.users"}),
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := postgres.NewForDialect(test.dialect, test.caps).
				GenerateMigrationAST(withDeclaredObjects(diff, desired))
			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, 1)

			index, ok := nodes[0].(*ast.IndexNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(index.Table, qt.Equals, "app.users")
			c.Assert(index.Name, qt.Equals, "idx_users_email")
		})
	}

	c.Assert(desired.Indexes[0].TableName, qt.Equals, "app.users")
}

func BenchmarkPlanner_LargeIndexReplacementPlan(b *testing.B) {
	c := qt.New(b)
	const indexCount = 10_000

	tables := make([]schemamodel.Table, 0, indexCount)
	indexes := make([]schemamodel.Index, 0, indexCount)
	additions := make([]difftypes.IndexRef, 0, indexCount)
	removals := make([]difftypes.IndexRef, 0, indexCount)
	for index := range indexCount {
		suffix := strconv.Itoa(index)
		structName := "Record" + suffix
		tableName := "records_" + suffix
		indexName := "idx_records_value_" + suffix
		tables = append(tables, schemamodel.Table{
			StructName: structName,
			Schema:     "app",
			Name:       tableName,
		})
		indexes = append(indexes, schemamodel.Index{
			StructName: structName,
			Name:       indexName,
			Fields:     []string{"value"},
		})
		additions = append(additions, difftypes.IndexRef{
			Name:      indexName,
			TableName: "app." + tableName,
		})
		removals = append(removals, difftypes.IndexRef{
			Name:      indexName,
			TableName: "app.legacy_" + suffix,
		})
	}
	desired := &schemamodel.Database{Tables: tables, Indexes: indexes}
	diff := &difftypes.SchemaDiff{IndexesAdded: difftypes.IndexAdditionsFor(desired, additions...), IndexesRemoved: removals}
	planner := postgres.New()

	b.ReportAllocs()
	b.ResetTimer()
	var nodes []ast.Node
	var err error
	for range b.N {
		nodes, err = planner.GenerateMigrationAST(withDeclaredObjects(diff, desired))
	}
	b.StopTimer()
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, indexCount*2)
}
