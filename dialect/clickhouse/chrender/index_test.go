package chrender_test

import (
	"math"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/clickhouse/chast"
	"ptah.run/dialect/clickhouse/chrender"
)

func TestSkippingIndexRenderingPreservesOperandsAndNames(t *testing.T) {
	for _, test := range []struct {
		name, table, index, expression, indexType string
		granularity                               uint64
		want                                      string
	}{
		{name: "defaults", table: "events", index: "idx", expression: "c", want: "ALTER TABLE `events` ADD INDEX `idx` c TYPE minmax GRANULARITY 1;"},
		{name: "qualified", table: "db.events", index: "idx", expression: "tuple(a, lower(b))", indexType: "bloom_filter(0.01)", granularity: 4, want: "ALTER TABLE `db`.`events` ADD INDEX `idx` tuple(a, lower(b)) TYPE bloom_filter(0.01) GRANULARITY 4;"},
		{name: "quoted dots", table: "`db.name`.`events.name`", index: "`idx.name`", expression: "c", want: "ALTER TABLE `db.name`.`events.name` ADD INDEX `idx.name` c TYPE minmax GRANULARITY 1;"},
		{name: "maximum granularity", table: "events", index: "idx", expression: "c", indexType: "set(100)", granularity: math.MaxUint64, want: "ALTER TABLE `events` ADD INDEX `idx` c TYPE set(100) GRANULARITY 18446744073709551615;"},
		{name: "embedded quotes", table: "`db``name`.`events``name`", index: "idx`name", expression: "c", want: "ALTER TABLE `db``name`.`events``name` ADD INDEX `idx``name` c TYPE minmax GRANULARITY 1;"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			registry := must.Must(chrender.Registry())
			ctx := renderer.ExtensionContext{Target: "clickhouse", Parent: &ast.AlterTableNode{Name: test.table}}
			statements, err := registry.Render(ctx, ast.AlterExtension, &chast.AddSkippingIndex{Name: test.index, Expression: test.expression, IndexType: test.indexType, Granularity: test.granularity})
			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{test.want})
		})
	}
}

func TestSkippingIndexRenderingRequiresCompletePayloadAndParent(t *testing.T) {
	c := qt.New(t)
	registry := must.Must(chrender.Registry())
	ctx := renderer.ExtensionContext{Target: "clickhouse", Parent: &ast.AlterTableNode{Name: "events"}}
	for _, payload := range []*chast.AddSkippingIndex{
		nil, {}, {Name: "idx"}, {Name: "idx", Expression: "c", IndexType: " "},
	} {
		statements, err := registry.Render(ctx, ast.AlterExtension, payload)
		c.Assert(err, qt.IsNotNil)
		c.Assert(statements, qt.HasLen, 0)
	}
	op := &chast.AddSkippingIndex{Name: "idx", Expression: "c"}
	ctx.Parent = nil
	statements, err := registry.Render(ctx, ast.AlterExtension, op)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(statements, qt.HasLen, 0)
	ctx.Parent = &ast.AlterTableNode{Name: "events"}
	statements, err = registry.Render(ctx, ast.StatementExtension, op)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(statements, qt.HasLen, 0)
}

func TestSkippingIndexDropRenderingQuotesNames(t *testing.T) {
	for _, test := range []struct {
		name, table, index, want string
	}{
		{name: "plain", table: "events", index: "idx", want: "ALTER TABLE `events` DROP INDEX `idx`;"},
		{name: "qualified", table: "db.events", index: "idx", want: "ALTER TABLE `db`.`events` DROP INDEX `idx`;"},
		{name: "embedded quotes", table: "`db``name`.`events``name`", index: "idx`name", want: "ALTER TABLE `db``name`.`events``name` DROP INDEX `idx``name`;"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			registry := must.Must(chrender.Registry())
			ctx := renderer.ExtensionContext{Target: "clickhouse", Parent: &ast.AlterTableNode{Name: test.table}}
			statements, err := registry.Render(ctx, ast.AlterExtension, &chast.DropSkippingIndex{Name: test.index})
			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{test.want})
		})
	}
}

func TestSkippingIndexDropRenderingRefusesInvalidPayloadsAndTargets(t *testing.T) {
	c := qt.New(t)
	registry := must.Must(chrender.Registry())
	ctx := renderer.ExtensionContext{Target: "clickhouse", Parent: &ast.AlterTableNode{Name: "events"}}
	for _, payload := range []*chast.DropSkippingIndex{nil, {}, {Name: " "}, {Name: "a\x00"}} {
		statements, err := registry.Render(ctx, ast.AlterExtension, payload)
		c.Assert(err, qt.IsNotNil)
		c.Assert(statements, qt.HasLen, 0)
	}
	ctx.Target = "postgres"
	statements, err := registry.Render(ctx, ast.AlterExtension, &chast.DropSkippingIndex{Name: "idx"})
	c.Assert(err, qt.IsNotNil)
	c.Assert(statements, qt.HasLen, 0)
}
