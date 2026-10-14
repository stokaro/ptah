package chrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chrender"
	"ptah.run/dialect/clickhouse/chschema"
)

// Settings the change sets come first and settings it resets last, and the
// sorting key shares its ALTER TABLE with the columns it adds.
func TestStorageRenderingWritesEveryChangedClause(t *testing.T) {
	c := qt.New(t)
	before := &chschema.ObservedTable{
		Engine: "MergeTree", OrderBy: "a, u", PrimaryKey: "a, u", TTL: "t + toIntervalDay(1)",
		Settings: "merge_with_ttl_timeout = 100, min_bytes_for_wide_part = 0, index_granularity = 8192",
	}
	after := before.Desired()
	after.OrderBy.Value = "a, u, k"
	after.SampleBy.Value = "u"
	after.TTL.Value = ""
	after.Settings.Value = "merge_with_ttl_timeout = 3600, ttl_only_drop_parts = 1"
	op := &chast.AlterStorage{Change: chdiff.Table{Before: before, After: after}, Columns: []chast.Column{
		{Name: "k", Type: "Int32", Comment: "it's the key"},
		{Name: "note", Type: "Nullable(String)", Value: "DEFAULT", Expression: "'n'"},
	}}
	ctx := renderer.ExtensionContext{Target: "clickhouse", Parent: &ast.AlterTableNode{Name: "events"}}
	statements, err := must.Must(chrender.Registry()).Render(ctx, ast.AlterExtension, op)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"ALTER TABLE events MODIFY SETTING merge_with_ttl_timeout = 3600, ttl_only_drop_parts = 1;",
		"ALTER TABLE events ADD COLUMN k Int32 COMMENT 'it''s the key', ADD COLUMN note Nullable(String) DEFAULT 'n', MODIFY ORDER BY (a, u, k);",
		"ALTER TABLE events MODIFY SAMPLE BY u;",
		"ALTER TABLE events REMOVE TTL;",
		"ALTER TABLE events RESET SETTING min_bytes_for_wide_part;",
	})
}

func TestStorageRenderingOfOneClause(t *testing.T) {
	before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "a, u", PrimaryKey: "a", SampleBy: "a", Settings: "a = 1"}
	for _, test := range []struct {
		name   string
		change func(*chschema.DesiredTable)
		want   string
	}{
		{name: "a dropped key expression", change: func(v *chschema.DesiredTable) { v.OrderBy.Value = "a" }, want: "ALTER TABLE events MODIFY ORDER BY a;"},
		{name: "a removed sampling key", change: func(v *chschema.DesiredTable) { v.SampleBy.Value = "" }, want: "ALTER TABLE events REMOVE SAMPLE BY;"},
		{name: "every setting reset", change: func(v *chschema.DesiredTable) { v.Settings.Value = "" }, want: "ALTER TABLE events RESET SETTING a;"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			after := before.Desired()
			test.change(after)
			ctx := renderer.ExtensionContext{Target: "clickhouse", Parent: &ast.AlterTableNode{Name: "events"}}
			statements, err := must.Must(chrender.Registry()).Render(ctx, ast.AlterExtension, &chast.AlterStorage{Change: chdiff.Table{Before: before, After: after}})
			c.Assert(err, qt.IsNil)
			c.Assert(statements[len(statements)-1], qt.Equals, test.want)
		})
	}
}

func TestStorageRenderingRefusesWithoutSQL(t *testing.T) {
	before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "a", PrimaryKey: "a"}
	partitioned := before.Desired()
	partitioned.PartitionBy.Value = "a"
	for _, test := range []struct {
		name   string
		target string
		op     *chast.AlterStorage
		want   error
	}{
		{name: "another target", target: "postgres", op: &chast.AlterStorage{Change: chdiff.Table{Before: before, After: partitioned}}, want: ptaherr.ErrUnsupportedDialect},
		{name: "a partition change", target: "clickhouse", op: &chast.AlterStorage{Change: chdiff.Table{Before: before, After: partitioned}}, want: ptaherr.ErrUnsupportedFeature},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := renderer.ExtensionContext{Target: test.target, Parent: &ast.AlterTableNode{Name: "events"}}
			statements, err := must.Must(chrender.Registry()).Render(ctx, ast.AlterExtension, test.op)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(statements, qt.HasLen, 0)
		})
	}
}
