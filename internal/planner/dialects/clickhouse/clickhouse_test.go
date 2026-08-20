package clickhouse_test

import (
	"fmt"
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := clickhouse.New().GenerateMigrationASTChecked(test.diff, &goschema.Database{})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

func TestGenerateMigrationASTChecked_DisabledViewsNeedNoDesiredDeclaration(t *testing.T) {
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
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
// pins the order and the count. A plan that emitted the create alone would be
// refused by the server, which answers "Table ... already exists" while the old
// object still owns the name, so the old query would stay in place.
//
// The one in-place edit ClickHouse does have is not usable here: measured on
// 26.7.3.19 and 24.10.4.191, `ALTER TABLE <mv> MODIFY QUERY` keeps the stored
// rows but refuses any select whose output columns differ, and a
// goschema.MaterializedView carries no column list for the planner to compare.
// See the comment on reportViewLikes.
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
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

// TestGenerateMigrationASTChecked_KindChangeDropsTheLiveObjectFirst pins the
// order for a name that changes kind without changing its name.
//
// The plain-view and materialized-view comparators are independent, so the same
// name arrives as an addition of the desired kind next to a removal of the live
// kind. ClickHouse resolves both against one namespace: measured on server
// 26.7.3.19, CREATE VIEW x with a materialized view named x already present
// answers "Code: 57. DB::Exception: Table ptah_test.x already exists.
// (TABLE_ALREADY_EXISTS)", and the materialized-over-plain direction answers
// the same. A plan that emitted the create first would therefore not apply.
//
// The two sides also spell the name differently, because the removal carries
// the catalog's qualified spelling while the addition carries the declaration's
// bare one. That pairing is what migration/schemadiff really produces here, so
// exact string matching would find no replacement at all.
func TestGenerateMigrationASTChecked_KindChangeDropsTheLiveObjectFirst(t *testing.T) {
	viewDesired := &goschema.Database{Views: []goschema.View{{
		StructName: "UserCounts",
		Name:       "user_counts",
		Body:       "SELECT count() AS c FROM analytics.users",
	}}}
	materializedDesired := &goschema.Database{MaterializedViews: []goschema.MaterializedView{{
		StructName: "UserCounts",
		Name:       "user_counts",
		Body:       "SELECT count() AS c FROM analytics.users",
	}}}

	tests := []struct {
		name      string
		diff      *types.SchemaDiff
		generated *goschema.Database
		wantNodes []ast.Node
	}{
		{
			name: "materialized view becomes a plain view",
			diff: &types.SchemaDiff{
				ViewsAdded:               []string{"user_counts"},
				MaterializedViewsRemoved: []string{"analytics.user_counts"},
			},
			generated: viewDesired,
			wantNodes: []ast.Node{
				&ast.DropMaterializedViewNode{Name: "analytics.user_counts", IfExists: true},
				&ast.CreateViewNode{
					Name: "user_counts",
					Body: "SELECT count() AS c FROM analytics.users",
				},
			},
		},
		{
			name: "plain view becomes a materialized view",
			diff: &types.SchemaDiff{
				MaterializedViewsAdded: []string{"user_counts"},
				ViewsRemoved:           []string{"analytics.user_counts"},
			},
			generated: materializedDesired,
			wantNodes: []ast.Node{
				&ast.DropViewNode{Name: "analytics.user_counts", IfExists: true},
				&ast.CreateMaterializedViewNode{
					Name: "user_counts",
					Body: "SELECT count() AS c FROM analytics.users",
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := clickhouse.New().GenerateMigrationASTChecked(test.diff, test.generated)

			c.Assert(err, qt.IsNil)
			// The whole plan, in order and exactly two statements: the
			// replacement is one drop moved in front of the create, never a
			// second statement naming the same object.
			c.Assert(nodes, qt.DeepEquals, test.wantNodes)
		})
	}
}

// TestGenerateMigrationASTChecked_UnrelatedRemovalStaysAfterTheCreates is the
// non-interference control for the kind-change reordering above.
//
// A removal whose name no addition claims keeps its place after the create
// pass, so the reordering cannot be satisfied by hoisting every drop.
func TestGenerateMigrationASTChecked_UnrelatedRemovalStaysAfterTheCreates(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{Views: []goschema.View{{
		StructName: "Reader",
		Name:       "analytics.reader",
		Body:       "SELECT id FROM analytics.users",
	}}}

	nodes, err := clickhouse.New().GenerateMigrationASTChecked(
		&types.SchemaDiff{
			ViewsAdded:               []string{"analytics.reader"},
			MaterializedViewsRemoved: []string{"analytics.user_counts"},
		},
		generated,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)
	create, ok := nodes[0].(*ast.CreateViewNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("expected the create first, got %T", nodes[0]))
	c.Assert(create.Name, qt.Equals, "analytics.reader")
	drop, ok := nodes[1].(*ast.DropMaterializedViewNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("expected the drop second, got %T", nodes[1]))
	c.Assert(drop.Name, qt.Equals, "analytics.user_counts")
}

func TestGenerateMigrationASTChecked_NilDiffFailurePath(t *testing.T) {
	p := clickhouse.New()
	tests := []struct {
		name      string
		generated *goschema.Database
	}{
		{name: "nil target"},
		{name: "empty target", generated: &goschema.Database{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := p.GenerateMigrationASTChecked(nil, test.generated)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// matViewRefreshDiff is a materialized view whose only change is its refresh
// schedule, with both sides supplied the way the comparator supplies them.
func matViewRefreshDiff(desired, current *ast.MatViewRefreshSpec) *types.SchemaDiff {
	return &types.SchemaDiff{
		MaterializedViewsModified: []types.MaterializedViewDiff{{
			ViewName:      "mv",
			Changes:       map[string]string{"refresh": "x -> y"},
			RefreshChange: &types.MatViewRefreshChange{Desired: desired, Current: current},
		}},
	}
}

func matViewRefreshSchema() *goschema.Database {
	return &goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			StructName: "MV",
			Name:       "mv",
			Body:       "SELECT count() AS c FROM src",
		}},
	}
}

// TestGenerateMigrationAST_RefreshOnlyChangeAltersInPlace is the reason the
// in-place path exists.
//
// A drop and a create would produce the right schedule and lose every row the
// view had accumulated -- ClickHouse's drop takes the inner storage table with
// it. Measured on 26.7.3.19: a view holding one row still holds it after
// `ALTER TABLE ... MODIFY REFRESH`, and the schedule is the new one
// (stokaro/ptah#1802).
func TestGenerateMigrationAST_RefreshOnlyChangeAltersInPlace(t *testing.T) {
	c := qt.New(t)
	diff := matViewRefreshDiff(
		&ast.MatViewRefreshSpec{Mode: "EVERY", Interval: "2 HOUR"},
		&ast.MatViewRefreshSpec{Mode: "EVERY", Interval: "1 HOUR"},
	)

	nodes, err := clickhouse.New().GenerateMigrationASTChecked(diff, matViewRefreshSchema())

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)
	alter, ok := nodes[0].(*ast.AlterMaterializedViewRefreshNode)
	c.Assert(ok, qt.IsTrue, qt.Commentf("got %T", nodes[0]))
	c.Assert(alter.Name, qt.Equals, "mv")
	c.Assert(alter.Refresh.Interval, qt.Equals, "2 HOUR")
	// The protected property, stated as an assertion rather than left implied:
	// nothing in this plan destroys the view.
	c.Assert(nodeKinds(nodes)["*ast.DropMaterializedViewNode"], qt.Equals, 0)
}

// nodeKinds counts the planned nodes by concrete type, so a test can assert
// which statements a plan contains without branching inside the assertion.
func nodeKinds(nodes []ast.Node) map[string]int {
	kinds := make(map[string]int, len(nodes))
	for _, node := range nodes {
		kinds[fmt.Sprintf("%T", node)]++
	}
	return kinds
}

// TestGenerateMigrationAST_RefreshTransitionsThatCannotBeAltered covers the
// three shapes the server will not change in place, each of which has to be a
// drop and a create instead.
//
// Measured: `ALTER TABLE <view> MODIFY REFRESH` against a PLAIN materialized
// view is answered `Code: 48 ... Alter of type 'MODIFY_REFRESH' is not
// supported by storage MaterializedView`, so a view gaining its first schedule
// or losing its last cannot take that path. A change that also touches the body
// cannot either, because the body is what a recreate exists to replace.
func TestGenerateMigrationAST_RefreshTransitionsThatCannotBeAltered(t *testing.T) {
	every := func(interval string) *ast.MatViewRefreshSpec {
		return &ast.MatViewRefreshSpec{Mode: "EVERY", Interval: interval}
	}
	tests := []struct {
		name string
		diff *types.SchemaDiff
	}{
		{
			name: "a plain view gaining its first schedule",
			diff: matViewRefreshDiff(every("1 HOUR"), nil),
		},
		{
			name: "a refreshable view losing its last",
			diff: matViewRefreshDiff(nil, every("1 HOUR")),
		},
		{
			name: "the body changed too",
			diff: func() *types.SchemaDiff {
				d := matViewRefreshDiff(every("2 HOUR"), every("1 HOUR"))
				d.MaterializedViewsModified[0].Changes["body"] = "a -> b"
				return d
			}(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := clickhouse.New().GenerateMigrationASTChecked(test.diff, matViewRefreshSchema())

			c.Assert(err, qt.IsNil)
			kinds := nodeKinds(nodes)
			c.Assert(kinds["*ast.AlterMaterializedViewRefreshNode"], qt.Equals, 0,
				qt.Commentf("MODIFY REFRESH cannot make this transition"))
			c.Assert(kinds["*ast.DropMaterializedViewNode"], qt.Not(qt.Equals), 0)
		})
	}
}
