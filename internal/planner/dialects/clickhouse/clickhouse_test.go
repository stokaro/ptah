package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/planner/dialects/clickhouse"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

func mkDB() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{
				StructName: "Event",
				Name:       "events",
				Overrides: map[string]map[string]string{
					"clickhouse": {
						"engine":   "MergeTree",
						"order_by": "id, created_at",
					},
				},
			},
		},
		Fields: []goschema.Field{
			{StructName: "Event", Name: "id", Type: "BIGINT", Primary: true, Nullable: false},
			{StructName: "Event", Name: "created_at", Type: "TIMESTAMP", Nullable: false},
			{StructName: "Event", Name: "payload", Type: "TEXT", Nullable: true},
		},
	}
}

func TestGenerateMigrationAST_AddTableDropTableAndAlter(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		TablesAdded:   []string{"events"},
		TablesRemoved: []string{"legacy"},
		TablesModified: []types.TableDiff{
			{
				TableName:    "existing",
				ColumnsAdded: []string{"new_col"},
				ColumnsModified: []types.ColumnDiff{
					{ColumnName: "id", Changes: map[string]string{"type": "Int64"}},
				},
				ColumnsRemoved: []string{"old_col"},
			},
		},
	}
	gen := mkDB()
	gen.Tables = append(gen.Tables, goschema.Table{StructName: "Existing", Name: "existing"})
	gen.Fields = append(gen.Fields,
		goschema.Field{StructName: "Existing", Name: "id", Type: "BIGINT", Primary: true, Nullable: false},
		goschema.Field{StructName: "Existing", Name: "new_col", Type: "INTEGER", Nullable: false},
	)

	p := clickhouse.New()
	nodes, err := p.GenerateMigrationASTChecked(diff, gen)
	c.Assert(err, qt.IsNil)

	// Expected order: CREATE events, ALTER existing (add), ALTER existing (modify),
	// ALTER existing (drop), DROP legacy.
	c.Assert(nodes, qt.HasLen, 5)

	ct, ok := nodes[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("first node should be CREATE TABLE, got %T", nodes[0]))
	c.Assert(ct.Name, qt.Equals, "events")

	alterAdd, ok := nodes[1].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("second node should be ALTER TABLE, got %T", nodes[1]))
	c.Assert(alterAdd.Name, qt.Equals, "existing")
	_, isAdd := alterAdd.Operations[0].(*ast.AddColumnOperation)
	c.Assert(isAdd, qt.IsTrue)

	alterMod, ok := nodes[2].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue)
	_, isMod := alterMod.Operations[0].(*ast.ModifyColumnOperation)
	c.Assert(isMod, qt.IsTrue)

	alterDrop, ok := nodes[3].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue)
	_, isDrop := alterDrop.Operations[0].(*ast.DropColumnOperation)
	c.Assert(isDrop, qt.IsTrue)

	drop, ok := nodes[4].(*ast.DropTableNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("last node should be DROP TABLE, got %T", nodes[4]))
	c.Assert(drop.Name, qt.Equals, "legacy")
	c.Assert(drop.IfExists, qt.IsTrue)
}

func TestGenerateMigrationAST_IndexAddRemove(t *testing.T) {
	c := qt.New(t)
	gen := mkDB()
	gen.Indexes = []goschema.Index{
		{StructName: "Event", Name: "idx_e_payload", Fields: []string{"payload"}},
	}
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "idx_e_payload", TableName: "events"},
		},
		IndexesRemoved: []types.IndexRef{
			{Name: "idx_old", TableName: "events"},
		},
	}

	p := clickhouse.New()
	nodes, err := p.GenerateMigrationASTChecked(diff, gen)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)
	idx, ok := nodes[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("expected IndexNode, got %T", nodes[0]))
	c.Assert(idx.Table, qt.Equals, "events")

	drop, ok := nodes[1].(*ast.DropIndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(drop.Table, qt.Equals, "events")
	c.Assert(drop.IfExists, qt.IsTrue)
}

func TestGenerateMigrationAST_EnumChangesAreSurfacedAsComment(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		EnumsAdded: []string{"status"},
	}
	p := clickhouse.New()
	nodes, err := p.GenerateMigrationASTChecked(diff, mkDB())
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)
	comment, ok := nodes[0].(*ast.CommentNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(comment.Text, qt.Contains, "enum changes")
}

func TestGenerateMigrationASTChecked_IndexUnresolvedStructRejected(t *testing.T) {
	c := qt.New(t)
	gen := mkDB()
	gen.Indexes = []goschema.Index{
		{StructName: "GhostStruct", Name: "idx_orphan", Fields: []string{"x"}},
	}
	diff := &types.SchemaDiff{IndexesAdded: []types.IndexRef{
		{Name: "idx_orphan", TableName: "ghosts"},
	}}

	p := clickhouse.New()
	nodes, err := p.GenerateMigrationASTChecked(diff, gen)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(nodes, qt.IsNil)
}

// TestGenerateMigrationAST_IndexExplicitTableNameWins verifies that when an
// index annotation carries `table=` we honor it without consulting the
// struct→table map. This is the supported escape hatch for cross-struct
// indexes.
func TestGenerateMigrationAST_IndexExplicitTableNameWins(t *testing.T) {
	c := qt.New(t)
	gen := mkDB()
	gen.Indexes = []goschema.Index{
		{StructName: "DoesNotMatter", Name: "idx_cross", Fields: []string{"x"}, TableName: "events"},
	}
	diff := &types.SchemaDiff{IndexesAdded: []types.IndexRef{
		{Name: "idx_cross", TableName: "events"},
	}}

	p := clickhouse.New()
	nodes, err := p.GenerateMigrationASTChecked(diff, gen)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)
	idx, ok := nodes[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(idx.Table, qt.Equals, "events")
}

// TestGenerateMigrationAST_IndexTypeAndGranularityPropagate guards the
// annotation-driven CH skipping-index path: type= and granularity= must
// reach the AST node so the renderer can emit the right SQL.
func TestGenerateMigrationAST_IndexTypeAndGranularityPropagate(t *testing.T) {
	c := qt.New(t)
	gen := mkDB()
	gen.Indexes = []goschema.Index{
		{
			StructName:  "Event",
			Name:        "idx_e_payload",
			Fields:      []string{"payload"},
			Type:        "bloom_filter(0.01)",
			Granularity: 64,
		},
	}
	diff := &types.SchemaDiff{IndexesAdded: []types.IndexRef{
		{Name: "idx_e_payload", TableName: "events"},
	}}

	p := clickhouse.New()
	nodes, err := p.GenerateMigrationASTChecked(diff, gen)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)
	idx, ok := nodes[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(idx.Type, qt.Equals, "bloom_filter(0.01)")
	c.Assert(idx.Granularity, qt.Equals, 64)
}

func TestGenerateMigrationASTChecked_NilSchemaHappyPath(t *testing.T) {
	c := qt.New(t)
	p := clickhouse.New()

	nodes, err := p.GenerateMigrationASTChecked(&types.SchemaDiff{}, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 0)
}

func TestGenerateMigrationASTChecked_MissingDesiredViewRejected(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name string
		diff *types.SchemaDiff
	}{
		{
			name: "addition",
			diff: &types.SchemaDiff{ViewsAdded: []string{"analytics.missing"}},
		},
		{
			name: "modification",
			diff: &types.SchemaDiff{ViewsModified: []types.ViewDiff{{ViewName: "analytics.missing"}}},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			nodes, err := clickhouse.New().GenerateMigrationASTChecked(test.diff, &goschema.Database{})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

func TestGenerateMigrationASTChecked_DisabledViewsNeedNoDesiredDeclaration(t *testing.T) {
	c := qt.New(t)
	planner := clickhouse.NewWithCapabilities(capability.Capabilities{})
	tests := []struct {
		name        string
		diff        *types.SchemaDiff
		wantReplace bool
	}{
		{
			name: "addition",
			diff: &types.SchemaDiff{ViewsAdded: []string{"analytics.missing"}},
		},
		{
			name:        "modification",
			diff:        &types.SchemaDiff{ViewsModified: []types.ViewDiff{{ViewName: "analytics.missing"}}},
			wantReplace: true,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			nodes, err := planner.GenerateMigrationASTChecked(test.diff, &goschema.Database{})

			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, 1)
			view, ok := nodes[0].(*ast.CreateViewNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(view.Name, qt.Equals, "analytics.missing")
			c.Assert(view.Body, qt.Equals, "")
			c.Assert(view.Replace, qt.Equals, test.wantReplace)
		})
	}
}

func TestNewWithCapabilities_NilIsConservative(t *testing.T) {
	c := qt.New(t)
	planner := clickhouse.NewWithCapabilities(nil)

	nodes, err := planner.GenerateMigrationASTChecked(
		&types.SchemaDiff{ViewsAdded: []string{"analytics.report"}},
		&goschema.Database{Views: []goschema.View{{
			Name: "analytics.report",
			Body: "SELECT 1",
		}}},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)
	view, ok := nodes[0].(*ast.CreateViewNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(view.Body, qt.Equals, "")
}

func TestGenerateMigrationAST_TableAdditionPreservesStructuralIdentity(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Literal", Name: "tenant.data"},
			{StructName: "Qualified", Schema: "tenant", Name: "data"},
		},
		Fields: []goschema.Field{
			{StructName: "Literal", Name: "id", Type: "UInt64"},
			{StructName: "Qualified", Name: "id", Type: "UInt64"},
		},
	}

	nodes, err := clickhouse.New().GenerateMigrationASTChecked(
		&types.SchemaDiff{TablesAdded: []string{"tenant.data"}},
		generated,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)
	table, ok := nodes[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(table.Name, qt.Equals, "tenant.data")
}

// TestGenerateMigrationASTChecked_MaterializedViewCreateCarriesItsBody pins the
// planner arm rather than the renderer: on master the diff produced a
// CreateMaterializedViewNode carrying identity only, which rendered as a
// diagnostic no matter what the renderer could emit. The body assertion is what
// separates a planned object from a named one.
func TestGenerateMigrationASTChecked_MaterializedViewCreateCarriesItsBody(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{MaterializedViews: []goschema.MaterializedView{{
		StructName: "UserCounts",
		Name:       "analytics.user_counts",
		Body:       "SELECT count() AS c FROM analytics.users",
	}}}

	nodes, err := clickhouse.New().GenerateMigrationASTChecked(
		&types.SchemaDiff{MaterializedViewsAdded: []string{"analytics.user_counts"}},
		generated,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)
	view, ok := nodes[0].(*ast.CreateMaterializedViewNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("expected CreateMaterializedViewNode, got %T", nodes[0]))
	c.Assert(view.Name, qt.Equals, "analytics.user_counts")
	c.Assert(view.Body, qt.Equals, "SELECT count() AS c FROM analytics.users")
}

// TestGenerateMigrationASTChecked_MaterializedViewChangeDropsBeforeCreating
// pins the order and the count. ClickHouse has no statement that edits the
// SELECT of an existing materialized view, so a plan that emitted the create
// alone would leave the old query in place on a server that already has the
// object.
func TestGenerateMigrationASTChecked_MaterializedViewChangeDropsBeforeCreating(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{MaterializedViews: []goschema.MaterializedView{{
		StructName: "UserCounts",
		Name:       "analytics.user_counts",
		Body:       "SELECT count() AS c FROM analytics.users WHERE active",
	}}}

	nodes, err := clickhouse.New().GenerateMigrationASTChecked(
		&types.SchemaDiff{MaterializedViewsModified: []types.MaterializedViewDiff{{
			ViewName: "analytics.user_counts",
			Changes:  map[string]string{"body": "old -> new"},
		}}},
		generated,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)
	drop, ok := nodes[0].(*ast.DropMaterializedViewNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("expected DropMaterializedViewNode first, got %T", nodes[0]))
	c.Assert(drop.Name, qt.Equals, "analytics.user_counts")
	c.Assert(drop.IfExists, qt.IsTrue)
	create, ok := nodes[1].(*ast.CreateMaterializedViewNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("expected CreateMaterializedViewNode second, got %T", nodes[1]))
	c.Assert(create.Body, qt.Equals, "SELECT count() AS c FROM analytics.users WHERE active")
}

// TestGenerateMigrationASTChecked_ViewReadingAMaterializedViewIsOrderedAfterIt
// pins that the two kinds share one dependency order rather than being planned
// one kind at a time.
//
// The diff lists the plain view first, so a planner that walked views and then
// materialized views would emit the reader before the object it reads, and
// ClickHouse refuses that: it resolves the query at CREATE time with
// "Unknown table expression identifier ... (UNKNOWN_TABLE)".
func TestGenerateMigrationASTChecked_ViewReadingAMaterializedViewIsOrderedAfterIt(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{
		Views: []goschema.View{{
			StructName: "Reader",
			Name:       "analytics.reader",
			Body:       "SELECT c FROM analytics.user_counts",
		}},
		MaterializedViews: []goschema.MaterializedView{{
			StructName: "UserCounts",
			Name:       "analytics.user_counts",
			Body:       "SELECT count() AS c FROM analytics.users",
		}},
	}

	nodes, err := clickhouse.New().GenerateMigrationASTChecked(
		&types.SchemaDiff{
			ViewsAdded:             []string{"analytics.reader"},
			MaterializedViewsAdded: []string{"analytics.user_counts"},
		},
		generated,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)
	base, ok := nodes[0].(*ast.CreateMaterializedViewNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("expected the materialized view first, got %T", nodes[0]))
	c.Assert(base.Name, qt.Equals, "analytics.user_counts")
	reader, ok := nodes[1].(*ast.CreateViewNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("expected the reading view second, got %T", nodes[1]))
	c.Assert(reader.Name, qt.Equals, "analytics.reader")
}

func TestGenerateMigrationASTChecked_MaterializedViewRemovalIsGuarded(t *testing.T) {
	c := qt.New(t)

	nodes, err := clickhouse.New().GenerateMigrationASTChecked(
		&types.SchemaDiff{MaterializedViewsRemoved: []string{"analytics.user_counts"}},
		&goschema.Database{},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)
	drop, ok := nodes[0].(*ast.DropMaterializedViewNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("expected DropMaterializedViewNode, got %T", nodes[0]))
	c.Assert(drop.Name, qt.Equals, "analytics.user_counts")
	c.Assert(drop.IfExists, qt.IsTrue)
}

func TestGenerateMigrationASTChecked_MissingDesiredMaterializedViewRejected(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name string
		diff *types.SchemaDiff
	}{
		{
			name: "addition",
			diff: &types.SchemaDiff{MaterializedViewsAdded: []string{"analytics.missing"}},
		},
		{
			name: "modification",
			diff: &types.SchemaDiff{MaterializedViewsModified: []types.MaterializedViewDiff{{
				ViewName: "analytics.missing",
			}}},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			nodes, err := clickhouse.New().GenerateMigrationASTChecked(test.diff, &goschema.Database{})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// TestGenerateMigrationASTChecked_DisabledMaterializedViewsNeedNoDeclaration
// keeps the diagnostic branch reachable: a capability set without
// materialized_views still names the object instead of failing on a desired
// declaration it was never going to read.
func TestGenerateMigrationASTChecked_DisabledMaterializedViewsNeedNoDeclaration(t *testing.T) {
	c := qt.New(t)
	planner := clickhouse.NewWithCapabilities(
		capability.ClickHouse24().With(capability.MaterializedViews, false),
	)
	tests := []struct {
		name string
		diff *types.SchemaDiff
	}{
		{
			name: "addition",
			diff: &types.SchemaDiff{MaterializedViewsAdded: []string{"analytics.missing"}},
		},
		{
			name: "modification",
			diff: &types.SchemaDiff{MaterializedViewsModified: []types.MaterializedViewDiff{{
				ViewName: "analytics.missing",
			}}},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			nodes, err := planner.GenerateMigrationASTChecked(test.diff, &goschema.Database{})

			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, 1)
			view, ok := nodes[0].(*ast.CreateMaterializedViewNode)
			c.Assert(ok, qt.IsTrue, qt.Commentf("expected CreateMaterializedViewNode, got %T", nodes[0]))
			c.Assert(view.Name, qt.Equals, "analytics.missing")
			c.Assert(view.Body, qt.Equals, "")
		})
	}
}

func TestGenerateMigrationASTChecked_NilDiffFailurePath(t *testing.T) {
	c := qt.New(t)
	p := clickhouse.New()
	tests := []struct {
		name      string
		generated *goschema.Database
	}{
		{name: "nil target"},
		{name: "empty target", generated: &goschema.Database{}},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			nodes, err := p.GenerateMigrationASTChecked(nil, test.generated)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
