package clickhouse_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/renderer/dialects/clickhouse"
)

func render(t *testing.T, nodes ...ast.Node) string {
	t.Helper()
	r := clickhouse.New()
	r.Reset()
	for _, n := range nodes {
		if err := n.Accept(r); err != nil {
			t.Fatalf("accept failed: %v", err)
		}
	}
	return r.Output()
}

func renderErr(nodes ...ast.Node) error {
	r := clickhouse.New()
	r.Reset()
	for _, n := range nodes {
		if err := n.Accept(r); err != nil {
			return err
		}
	}
	return nil
}

// makeMergeTreeTable builds a typical events table that exercises the engine
// options most of the tests in this file want to assert on.
func makeMergeTreeTable() *ast.CreateTableNode {
	t := ast.NewCreateTable("events").
		AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary()).
		AddColumn(ast.NewColumn("created_at", "TIMESTAMP").SetNotNull()).
		AddColumn(ast.NewColumn("payload", "TEXT"))
	t.SetOption("ENGINE", "MergeTree")
	t.SetOption("ORDER_BY", "id, created_at")
	t.SetOption("PARTITION_BY", "toYYYYMM(created_at)")
	t.SetOption("PRIMARY_KEY", "id")
	t.SetOption("SAMPLE_BY", "id")
	t.SetOption("TTL", "created_at + INTERVAL 1 MONTH")
	t.SetOption("SETTINGS", "index_granularity = 8192")
	return t
}

func TestCreateTable_MergeTreeFullEngineSpec(t *testing.T) {
	c := qt.New(t)
	out := render(t, makeMergeTreeTable())

	c.Assert(out, qt.Contains, "CREATE TABLE events")
	c.Assert(out, qt.Contains, "ENGINE = MergeTree")
	c.Assert(out, qt.Contains, "PARTITION BY toYYYYMM(created_at)")
	c.Assert(out, qt.Contains, "ORDER BY (id, created_at)")
	c.Assert(out, qt.Contains, "PRIMARY KEY (id)")
	c.Assert(out, qt.Contains, "SAMPLE BY id")
	c.Assert(out, qt.Contains, "TTL created_at + INTERVAL 1 MONTH")
	c.Assert(out, qt.Contains, "SETTINGS index_granularity = 8192")

	// id is PRIMARY -> NOT Nullable; created_at is NOT NULL -> NOT Nullable;
	// payload is nullable by default -> wrapped.
	c.Assert(out, qt.Contains, "id Int64")
	c.Assert(out, qt.Contains, "created_at DateTime64(3)")
	c.Assert(out, qt.Contains, "payload Nullable(String)")
}

func TestCreateTable_DefaultsToMergeTreeWithPrimaryKeyOrderBy(t *testing.T) {
	c := qt.New(t)
	tbl := ast.NewCreateTable("widgets").
		AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary())

	out := render(t, tbl)
	c.Assert(out, qt.Contains, "ENGINE = MergeTree")
	// Falls back to the PK as ORDER BY because the user didn't supply one.
	c.Assert(out, qt.Contains, "ORDER BY (id)")
}

func TestCreateTable_MergeTreeMissingOrderByAndPK(t *testing.T) {
	c := qt.New(t)
	tbl := ast.NewCreateTable("orphan").
		AddColumn(ast.NewColumn("id", "BIGINT").SetNotNull())
	tbl.SetOption("ENGINE", "MergeTree")

	err := renderErr(tbl)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "ORDER BY")
}

func TestCreateTable_NonMergeTreeEngineDoesNotRequireOrderBy(t *testing.T) {
	c := qt.New(t)
	tbl := ast.NewCreateTable("kv").
		AddColumn(ast.NewColumn("k", "STRING").SetNotNull()).
		AddColumn(ast.NewColumn("v", "STRING"))
	tbl.SetOption("ENGINE", "Memory")

	out := render(t, tbl)
	c.Assert(out, qt.Contains, "ENGINE = Memory")
	c.Assert(out, qt.Not(qt.Contains), "ORDER BY")
}

func TestCreateTable_PrimaryKeyNotPrefixOfOrderBy(t *testing.T) {
	c := qt.New(t)
	tbl := ast.NewCreateTable("bad_pk").
		AddColumn(ast.NewColumn("id", "BIGINT").SetNotNull()).
		AddColumn(ast.NewColumn("ts", "TIMESTAMP").SetNotNull())
	tbl.SetOption("ENGINE", "MergeTree")
	tbl.SetOption("ORDER_BY", "id, ts")
	// PK ts is NOT a prefix of (id, ts).
	tbl.SetOption("PRIMARY_KEY", "ts")

	err := renderErr(tbl)
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "prefix of ORDER BY")
}

func TestCreateTable_PrimaryKeyPrefixIsAccepted(t *testing.T) {
	c := qt.New(t)
	tbl := ast.NewCreateTable("good_pk").
		AddColumn(ast.NewColumn("a", "INTEGER").SetNotNull()).
		AddColumn(ast.NewColumn("b", "INTEGER").SetNotNull())
	tbl.SetOption("ENGINE", "MergeTree")
	tbl.SetOption("ORDER_BY", "a, b")
	tbl.SetOption("PRIMARY_KEY", "a")

	out := render(t, tbl)
	c.Assert(out, qt.Contains, "ORDER BY (a, b)")
	c.Assert(out, qt.Contains, "PRIMARY KEY (a)")
}

func TestCreateTable_DefaultsAndCommentsRendered(t *testing.T) {
	c := qt.New(t)
	tbl := ast.NewCreateTable("with_defaults").
		AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary())
	lit := ast.NewColumn("status", "VARCHAR(20)").SetNotNull()
	lit.Default = &ast.DefaultValue{Value: "active"}
	lit.Comment = "lifecycle status"
	tbl.AddColumn(lit)
	exp := ast.NewColumn("created_at", "TIMESTAMP")
	exp.Default = &ast.DefaultValue{Expression: "now()"}
	tbl.AddColumn(exp)

	out := render(t, tbl)
	c.Assert(out, qt.Contains, "status String DEFAULT 'active' COMMENT 'lifecycle status'")
	c.Assert(out, qt.Contains, "created_at Nullable(DateTime64(3)) DEFAULT now()")
}

func TestCreateTable_DefaultValueEscapesQuotes(t *testing.T) {
	c := qt.New(t)
	tbl := ast.NewCreateTable("q").
		AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary())
	col := ast.NewColumn("note", "TEXT").SetNotNull()
	col.Default = &ast.DefaultValue{Value: "it's fine"}
	tbl.AddColumn(col)

	out := render(t, tbl)
	c.Assert(out, qt.Contains, "DEFAULT 'it''s fine'")
}

func TestCreateTable_CheckConstraintRendered(t *testing.T) {
	c := qt.New(t)
	tbl := ast.NewCreateTable("with_check").
		AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary()).
		AddColumn(ast.NewColumn("qty", "INTEGER").SetNotNull())
	tbl.AddConstraint(&ast.ConstraintNode{Type: ast.CheckConstraint, Name: "qty_pos", Expression: "qty > 0"})

	out := render(t, tbl)
	c.Assert(out, qt.Contains, "CONSTRAINT qty_pos CHECK (qty > 0)")
}

func TestCreateTable_ForeignKeyAndUniqueAreSilentlyDropped(t *testing.T) {
	c := qt.New(t)
	tbl := ast.NewCreateTable("fk_table").
		AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary()).
		AddColumn(ast.NewColumn("other_id", "BIGINT").SetNotNull())
	tbl.AddConstraint(&ast.ConstraintNode{
		Type:    ast.ForeignKeyConstraint,
		Name:    "fk_other",
		Columns: []string{"other_id"},
		Reference: &ast.ForeignKeyRef{
			Table:  "others",
			Column: "id",
		},
	})
	tbl.AddConstraint(ast.NewUniqueConstraint("uq_other", "other_id"))

	out := render(t, tbl)
	c.Assert(out, qt.Not(qt.Contains), "FOREIGN KEY")
	c.Assert(out, qt.Not(qt.Contains), "UNIQUE")
}

func TestColumnTypeMapping(t *testing.T) {
	cases := []struct {
		name     string
		col      *ast.ColumnNode
		want     string
		wantErr  bool
		contains string
	}{
		{name: "varchar to String", col: ast.NewColumn("c", "VARCHAR(255)").SetNotNull(), want: "c String"},
		{name: "text to String", col: ast.NewColumn("c", "TEXT").SetNotNull(), want: "c String"},
		{name: "int4 to Int32", col: ast.NewColumn("c", "INTEGER").SetNotNull(), want: "c Int32"},
		{name: "bigint to Int64", col: ast.NewColumn("c", "BIGINT").SetNotNull(), want: "c Int64"},
		{name: "smallint to Int16", col: ast.NewColumn("c", "SMALLINT").SetNotNull(), want: "c Int16"},
		{name: "bool to Bool", col: ast.NewColumn("c", "BOOLEAN").SetNotNull(), want: "c Bool"},
		{name: "timestamp to DateTime64(3)", col: ast.NewColumn("c", "TIMESTAMP").SetNotNull(), want: "c DateTime64(3)"},
		{name: "date to Date", col: ast.NewColumn("c", "DATE").SetNotNull(), want: "c Date"},
		{name: "double to Float64", col: ast.NewColumn("c", "DOUBLE").SetNotNull(), want: "c Float64"},
		{name: "real to Float32", col: ast.NewColumn("c", "REAL").SetNotNull(), want: "c Float32"},
		{name: "numeric(p,s) to Decimal", col: ast.NewColumn("c", "NUMERIC(12,4)").SetNotNull(), want: "c Decimal(12,4)"},
		{name: "bytea to String", col: ast.NewColumn("c", "BYTEA").SetNotNull(), want: "c String"},
		{name: "nullable wrapping", col: ast.NewColumn("c", "INTEGER"), want: "c Nullable(Int32)"},
		{name: "serial errors", col: ast.NewColumn("c", "SERIAL").SetNotNull(), wantErr: true, contains: "auto-increment"},
		{name: "time errors", col: ast.NewColumn("c", "TIME").SetNotNull(), wantErr: true, contains: "TIME"},
		{name: "native CH type passthrough", col: ast.NewColumn("c", "LowCardinality(String)").SetNotNull(), want: "c LowCardinality(String)"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			tbl := ast.NewCreateTable("t").AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary())
			tbl.AddColumn(tc.col)
			err := renderErr(tbl)
			if tc.wantErr {
				c.Assert(err, qt.IsNotNil)
				c.Assert(err.Error(), qt.Contains, tc.contains)
				return
			}
			c.Assert(err, qt.IsNil)
			out := render(t, tbl)
			c.Assert(out, qt.Contains, tc.want)
		})
	}
}

func TestAlterTable_Operations(t *testing.T) {
	c := qt.New(t)

	addCol := &ast.AlterTableNode{
		Name: "events",
		Operations: []ast.AlterOperation{
			&ast.AddColumnOperation{Column: ast.NewColumn("source", "VARCHAR(64)").SetNotNull()},
		},
	}
	dropCol := &ast.AlterTableNode{
		Name:       "events",
		Operations: []ast.AlterOperation{&ast.DropColumnOperation{ColumnName: "payload"}},
	}
	modCol := &ast.AlterTableNode{
		Name: "events",
		Operations: []ast.AlterOperation{
			&ast.ModifyColumnOperation{Column: ast.NewColumn("source", "TEXT").SetNotNull()},
		},
	}
	addCheck := &ast.AlterTableNode{
		Name: "events",
		Operations: []ast.AlterOperation{
			&ast.AddConstraintOperation{Constraint: &ast.ConstraintNode{Type: ast.CheckConstraint, Name: "src_set", Expression: "source <> ''"}},
		},
	}
	dropCheck := &ast.AlterTableNode{
		Name:       "events",
		Operations: []ast.AlterOperation{&ast.DropConstraintOperation{ConstraintName: "src_set"}},
	}

	out := render(t, addCol, dropCol, modCol, addCheck, dropCheck)
	c.Assert(out, qt.Contains, "ALTER TABLE events ADD COLUMN source String;")
	c.Assert(out, qt.Contains, "ALTER TABLE events DROP COLUMN payload;")
	c.Assert(out, qt.Contains, "ALTER TABLE events MODIFY COLUMN source String;")
	c.Assert(out, qt.Contains, "ALTER TABLE events ADD CONSTRAINT src_set CHECK (source <> '');")
	c.Assert(out, qt.Contains, "ALTER TABLE events DROP CONSTRAINT src_set;")
}

func TestAlterTable_NonCheckConstraintEmitsNotSupportedComment(t *testing.T) {
	c := qt.New(t)
	alter := &ast.AlterTableNode{
		Name: "events",
		Operations: []ast.AlterOperation{
			&ast.AddConstraintOperation{Constraint: &ast.ConstraintNode{Type: ast.ForeignKeyConstraint, Name: "fk_x", Columns: []string{"x"}, Reference: &ast.ForeignKeyRef{Table: "y", Column: "id"}}},
		},
	}
	out := render(t, alter)
	c.Assert(out, qt.Contains, "-- CLICKHOUSE:")
	c.Assert(out, qt.Not(qt.Contains), "FOREIGN KEY")
}

func TestDropTable(t *testing.T) {
	c := qt.New(t)
	out := render(t, ast.NewDropTable("events").SetIfExists().SetComment("WARNING: data loss"))
	c.Assert(out, qt.Contains, "-- WARNING: data loss")
	c.Assert(out, qt.Contains, "DROP TABLE IF EXISTS events;")
}

func TestDropTable_WithoutIfExists(t *testing.T) {
	c := qt.New(t)
	out := render(t, ast.NewDropTable("events"))
	c.Assert(out, qt.Contains, "DROP TABLE events;")
}

func TestVisitIndex_DefaultsToMinmaxSkippingIndex(t *testing.T) {
	c := qt.New(t)
	idx := ast.NewIndex("idx_e_src", "events", "source")
	out := render(t, idx)
	c.Assert(out, qt.Contains, "ALTER TABLE events ADD INDEX idx_e_src source TYPE minmax GRANULARITY 8192;")
}

func TestVisitIndex_MultiColumnExpression(t *testing.T) {
	c := qt.New(t)
	idx := ast.NewIndex("idx_e_src_ts", "events", "source", "ts")
	out := render(t, idx)
	c.Assert(out, qt.Contains, "(source, ts)")
}

func TestVisitDropIndex_RequiresTable(t *testing.T) {
	c := qt.New(t)
	out := render(t, ast.NewDropIndex("idx_orphan"))
	// We expect a comment line explaining why the drop is being skipped, and
	// no executable ALTER statement. The comment itself mentions
	// `ALTER TABLE ... DROP INDEX` as the required form, so to assert the
	// absence of a real statement we look for a non-comment line.
	c.Assert(out, qt.Contains, "-- CLICKHOUSE:")
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		t.Fatalf("expected only comment output, got executable line: %q", line)
	}
}

func TestVisitDropIndex_OnTable(t *testing.T) {
	c := qt.New(t)
	out := render(t, ast.NewDropIndex("idx_e_src").SetTable("events"))
	c.Assert(out, qt.Contains, "ALTER TABLE events DROP INDEX idx_e_src;")
}

func TestUnsupportedFeaturesEmitCommentAndReturnNil(t *testing.T) {
	cases := []struct {
		name string
		node ast.Node
	}{
		{"enum", ast.NewEnum("status", "a", "b")},
		{"extension", ast.NewExtension("pg_trgm")},
		{"drop extension", &ast.DropExtensionNode{Name: "pg_trgm"}},
		{"create function", &ast.CreateFunctionNode{Name: "fn"}},
		{"drop function", &ast.DropFunctionNode{Name: "fn"}},
		{"create policy", &ast.CreatePolicyNode{Name: "p", Table: "t"}},
		{"drop policy", &ast.DropPolicyNode{Name: "p", Table: "t"}},
		{"enable rls", &ast.AlterTableEnableRLSNode{Table: "t"}},
		{"disable rls", &ast.AlterTableDisableRLSNode{Table: "t"}},
		{"create role", &ast.CreateRoleNode{Name: "r"}},
		{"drop role", &ast.DropRoleNode{Name: "r"}},
		{"alter role", &ast.AlterRoleNode{Name: "r"}},
		{"create type", &ast.CreateTypeNode{Name: "tp"}},
		{"alter type", &ast.AlterTypeNode{Name: "tp"}},
		{"drop type", &ast.DropTypeNode{Name: "tp"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			err := renderErr(tc.node)
			c.Assert(err, qt.IsNil)
			out := render(t, tc.node)
			c.Assert(strings.Contains(out, "-- CLICKHOUSE:"), qt.IsTrue, qt.Commentf("expected '-- CLICKHOUSE:' marker, got: %q", out))
		})
	}
}

func TestVisitComment(t *testing.T) {
	c := qt.New(t)
	out := render(t, ast.NewComment("a note"))
	c.Assert(out, qt.Contains, "-- a note --")
}

func TestVisitRawSQL_PassThrough(t *testing.T) {
	c := qt.New(t)
	out := render(t, ast.NewRawSQL("SELECT 1;"))
	c.Assert(out, qt.Contains, "SELECT 1;")
}

func TestDialectAndOutputHelpers(t *testing.T) {
	c := qt.New(t)
	r := clickhouse.New()
	c.Assert(r.Dialect(), qt.Equals, "clickhouse")
	c.Assert(r.GetDialect(), qt.Equals, "clickhouse")
	out, err := r.Render(ast.NewComment("hello"))
	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "-- hello --")
	c.Assert(r.GetOutput(), qt.Equals, r.Output())
}
