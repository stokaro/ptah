package ydbscheme_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbscheme"
)

// TestCommonEffects_AColumnTableReadsTheSourcesItsTTLMovesRowsTo gives a
// column table's creation a read of each external data source its tiered TTL
// names, so the source's owner creates the source first and drops it after. A
// path written absolute names the source relative to the database root; one
// outside the root, or written absolute where the root is not known, reads
// nothing this plan manages, and neither does a tier that deletes rows.
func TestCommonEffects_AColumnTableReadsTheSourcesItsTTLMovesRowsTo(t *testing.T) {
	builder := objectidentity.NewBuilder(identifier.ForDialect("ydb"))
	table := func(sources ...string) *ast.CreateTableNode {
		policy := &ast.YDBTieredTTLSpec{Column: "at"}
		for _, source := range sources {
			policy.Tiers = append(policy.Tiers, ast.YDBTTLTierSpec{Interval: "P1D", ExternalSource: source})
		}
		return &ast.CreateTableNode{Name: "app.events", YDBColumnTable: &ast.YDBColumnTableSpec{TTL: policy}}
	}
	created := []plangraph.Effect{
		{Subject: ydbscheme.Path("app", "events"), Action: plangraph.Create},
		{Subject: builder.Table("app.events"), Action: plangraph.Create},
	}
	tests := []struct {
		name string
		root string
		node ast.Node
		want []plangraph.Effect
	}{
		{name: "a relative path and a deleting tier", node: table("ext/archive", ""),
			want: append(created[:2:2], plangraph.Effect{Subject: ydbexternal.SourceRef("ext", "archive"), Action: plangraph.Read})},
		{name: "an absolute path under the root, and the same source again", root: "/local", node: table("/local/ext/archive", "ext/archive"),
			want: append(created[:2:2], plangraph.Effect{Subject: ydbexternal.SourceRef("ext", "archive"), Action: plangraph.Read})},
		{name: "an absolute path with no root", node: table("/local/ext/archive"), want: created},
		{name: "a path outside the root", root: "/local", node: table("/other/archive"), want: created},
		{name: "a row table", node: &ast.CreateTableNode{Name: "app.events"}, want: created},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			effects, err := ydbscheme.CommonEffects(builder, test.root, test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(effects, qt.DeepEquals, test.want)
		})
	}
}
