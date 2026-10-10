package ydbscheme_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/dialect/ydb/ydbtopic"
)

// TestCommonEffects_ReadTheTopicATransferReads gives a transfer of a topic of
// this database a read of that topic, so the topic's owner creates or changes
// it first and drops it last. A path written absolute is read against the
// database root; one outside it, or written absolute where the root is not
// known, names no topic the plan manages and reads nothing, and so does a
// transfer from another database.
func TestCommonEffects_ReadTheTopicATransferReads(t *testing.T) {
	builder := objectidentity.NewBuilder(identifier.ForDialect("ydb"))
	tests := []struct {
		name string
		root string
		node ast.Node
		want []plangraph.Effect
	}{
		{name: "a created transfer", node: &ast.CreateTransferNode{Name: "app.move", Spec: ast.TransferSpec{Source: "app/events.v1"}},
			want: []plangraph.Effect{{Subject: ydbscheme.Path("app", "move"), Action: plangraph.Create},
				{Subject: ydbtopic.Ref("app", "events.v1"), Action: plangraph.Read}}},
		{name: "a changed transfer", node: &ast.AlterTransferNode{Name: "app.move", Spec: ast.TransferSpec{Source: "events"}},
			want: []plangraph.Effect{{Subject: ydbscheme.Path("app", "move"), Action: plangraph.Alter},
				{Subject: ydbtopic.Ref("", "events"), Action: plangraph.Read}}},
		{name: "a dropped transfer", node: &ast.DropTransferNode{Name: "app.move", Topic: "app/events"},
			want: []plangraph.Effect{{Subject: ydbscheme.Path("app", "move"), Action: plangraph.Drop},
				{Subject: ydbtopic.Ref("app", "events"), Action: plangraph.Read}}},
		{name: "an absolute path in the database", root: "/local", node: &ast.CreateTransferNode{Name: "app.move",
			Spec: ast.TransferSpec{Source: "/local/app/events"}},
			want: []plangraph.Effect{{Subject: ydbscheme.Path("app", "move"), Action: plangraph.Create},
				{Subject: ydbtopic.Ref("app", "events"), Action: plangraph.Read}}},
		{name: "an absolute path in another database", root: "/local", node: &ast.CreateTransferNode{Name: "app.move",
			Spec: ast.TransferSpec{Source: "/other/app/events"}},
			want: []plangraph.Effect{{Subject: ydbscheme.Path("app", "move"), Action: plangraph.Create}}},
		{name: "an absolute path, the root unknown", node: &ast.CreateTransferNode{Name: "app.move",
			Spec: ast.TransferSpec{Source: "/local/app/events"}},
			want: []plangraph.Effect{{Subject: ydbscheme.Path("app", "move"), Action: plangraph.Create}}},
		{name: "a transfer from another database", node: &ast.CreateTransferNode{Name: "app.move", Spec: ast.TransferSpec{Source: "app/events",
			Connection: ast.ReplicationConnectionSpec{ConnectionString: "grpc://primary:2136/?database=/prod"}}},
			want: []plangraph.Effect{{Subject: ydbscheme.Path("app", "move"), Action: plangraph.Create}}},
		{name: "a dropped transfer from another database", node: &ast.DropTransferNode{Name: "app.move"},
			want: []plangraph.Effect{{Subject: ydbscheme.Path("app", "move"), Action: plangraph.Drop}}},
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
