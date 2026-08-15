package postgres_test

import (
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestPlanner_IndexRefs_QualifiesDropsAndReplacesOnlyExactRef(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "idx_shared", TableName: "app.records"},
			{Name: "idx_shared", TableName: "logs.records"},
		},
		IndexesRemoved: []types.IndexRef{
			{Name: "idx_shared", TableName: "app.records"},
			{Name: "idx_shared", TableName: "archive.records"},
		},
	}
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "records", Schema: "app", StructName: "AppRecord"},
			{Name: "records", Schema: "logs", StructName: "LogRecord"},
		},
		Indexes: []goschema.Index{
			{Name: "idx_shared", StructName: "LogRecord", Fields: []string{"recorded_at"}},
			{Name: "idx_shared", StructName: "AppRecord", Fields: []string{"external_id"}},
		},
	}

	nodes, err := postgres.New().GenerateMigrationASTChecked(diff, generated)
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
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "idx_shared", TableName: "app.orders"},
		},
		IndexesRemoved: []types.IndexRef{
			{Name: "idx_shared", TableName: "app.users"},
		},
	}
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "orders", Schema: "app", StructName: "Order"},
		},
		Indexes: []goschema.Index{
			{Name: "idx_shared", StructName: "Order", Fields: []string{"reference"}},
		},
	}

	nodes, err := postgres.New().GenerateMigrationASTChecked(diff, generated)
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
	diff := &types.SchemaDiff{}
	diff.SetIndexAdditions([]types.IndexRef{
		{Name: "idx_shared", TableName: "public.users"},
	})
	diff.SetIndexRemovals([]types.IndexRef{
		{Name: "idx_shared", TableName: "public.users"},
	})
	generated := &goschema.Database{
		Indexes: []goschema.Index{
			{Name: "idx_shared", TableName: "public.users", Fields: []string{"handle"}},
		},
	}

	nodes, err := postgres.NewForDialect(platform.CockroachDB, capability.CockroachDB23()).
		GenerateMigrationASTChecked(diff, generated)
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
	diff := &types.SchemaDiff{}
	diff.SetIndexAdditions([]types.IndexRef{
		{Name: "idx_shared", TableName: "app.orders"},
	})
	diff.SetIndexRemovals([]types.IndexRef{
		{Name: "idx_shared", TableName: "app.users"},
	})
	generated := &goschema.Database{
		Indexes: []goschema.Index{
			{Name: "idx_shared", TableName: "app.orders", Fields: []string{"reference"}},
		},
	}

	nodes, err := postgres.NewForDialect(platform.Spanner, capability.SpannerPostgres()).
		GenerateMigrationASTChecked(diff, generated)
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
	diff := &types.SchemaDiff{}
	diff.SetIndexAdditions([]types.IndexRef{
		{Name: "idx_shared", TableName: "app.orders"},
	})
	diff.SetIndexRemovals([]types.IndexRef{
		{Name: "idx_shared", TableName: "logs.users"},
	})
	generated := &goschema.Database{
		Indexes: []goschema.Index{
			{Name: "idx_shared", TableName: "app.orders", Fields: []string{"reference"}},
		},
	}

	nodes, err := postgres.NewForDialect(platform.Spanner, capability.SpannerPostgres()).
		GenerateMigrationASTChecked(diff, generated)
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
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "idx_users_email", TableName: "app.users"},
		},
	}
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Shared", Schema: "app", Name: "users"},
			{StructName: "Shared", Schema: "archive", Name: "records"},
		},
		Indexes: []goschema.Index{
			{
				Name:       "idx_users_email",
				StructName: "Shared",
				TableName:  "app.users",
				Fields:     []string{"email"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := postgres.NewForDialect(test.dialect, test.caps).
				GenerateMigrationASTChecked(diff, generated)
			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, 1)

			index, ok := nodes[0].(*ast.IndexNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(index.Table, qt.Equals, "app.users")
			c.Assert(index.Name, qt.Equals, "idx_users_email")
		})
	}

	c.Assert(generated.Indexes[0].TableName, qt.Equals, "app.users")
}

func BenchmarkPlanner_LargeIndexReplacementPlan(b *testing.B) {
	c := qt.New(b)
	const indexCount = 10_000

	tables := make([]goschema.Table, 0, indexCount)
	indexes := make([]goschema.Index, 0, indexCount)
	additions := make([]types.IndexRef, 0, indexCount)
	removals := make([]types.IndexRef, 0, indexCount)
	for index := range indexCount {
		suffix := strconv.Itoa(index)
		structName := "Record" + suffix
		tableName := "records_" + suffix
		indexName := "idx_records_value_" + suffix
		tables = append(tables, goschema.Table{
			StructName: structName,
			Schema:     "app",
			Name:       tableName,
		})
		indexes = append(indexes, goschema.Index{
			StructName: structName,
			Name:       indexName,
			Fields:     []string{"value"},
		})
		additions = append(additions, types.IndexRef{
			Name:      indexName,
			TableName: "app." + tableName,
		})
		removals = append(removals, types.IndexRef{
			Name:      indexName,
			TableName: "app.legacy_" + suffix,
		})
	}
	generated := &goschema.Database{Tables: tables, Indexes: indexes}
	diff := &types.SchemaDiff{IndexesAdded: additions, IndexesRemoved: removals}
	planner := postgres.New()

	b.ReportAllocs()
	b.ResetTimer()
	var nodes []ast.Node
	var err error
	for range b.N {
		nodes, err = planner.GenerateMigrationASTChecked(diff, generated)
	}
	b.StopTimer()
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, indexCount*2)
}
