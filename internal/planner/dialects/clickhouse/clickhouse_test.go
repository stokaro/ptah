package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
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
