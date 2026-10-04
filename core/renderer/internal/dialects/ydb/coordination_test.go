package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer/internal/dialects/ydb"
)

// The YDB renderer checks the coordination_nodes key itself, for each of the
// three statements. The renderer core checks the key before a visit reaches
// a dialect, so only a caller that renders with this renderer directly meets
// this check; the core's own is covered in its package.
func TestRender_CoordinationNode_FailurePath(t *testing.T) {
	caps := capability.YDB262().With(capability.CoordinationNodes, false)
	const want = "coordination node app.locks, which requires target capability coordination_nodes, " +
		"unavailable on this ydb target"
	tests := []struct {
		name string
		node ast.Node
	}{
		{name: "a creation", node: &ast.CreateCoordinationNodeNode{Name: "app.locks"}},
		{
			name: "a change",
			node: &ast.AlterCoordinationNodeNode{Name: "app.locks", Spec: ast.CoordinationNodeSpec{ReadConsistencyMode: "strict"}},
		},
		{name: "a drop", node: &ast.DropCoordinationNodeNode{Name: "app.locks"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql, err := ydb.NewWithCapabilities(caps).Render(test.node)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, want)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
