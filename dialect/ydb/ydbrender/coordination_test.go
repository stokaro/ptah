package ydbrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbrender"
)

func TestCoordinationRenderingUsesCapturedTransitionWithoutATable(t *testing.T) {
	cases := []struct {
		name  string
		value *ydbast.CoordinationNode
		want  string
	}{
		{name: "create raw defaults", value: &ydbast.CoordinationNode{Name: "literal.dot", Change: ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}}, want: "CREATE COORDINATION NODE `literal.dot`;"},
		{name: "reset changed fields", value: &ydbast.CoordinationNode{Schema: "app", Name: "locks", Change: ydbdiff.CoordinationNode{
			Before: &ydbcoordination.Observed{Spec: ydbcoordination.Spec{ReadConsistencyMode: "strict", SelfCheckPeriodMillis: 1000}}, After: &ydbcoordination.Desired{}}}, want: "ALTER COORDINATION NODE `app/locks` SET (read_consistency_mode = 'relaxed');"},
		{name: "drop", value: &ydbast.CoordinationNode{Schema: "app", Name: "locks", Change: ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}}}, want: "DROP COORDINATION NODE `app/locks`;"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			registry, err := renderer.NewExtensions(ydbrender.CoordinationHandler())
			c.Assert(err, qt.IsNil)
			result, err := registry.Render(renderer.ExtensionContext{Target: "ydb", Capabilities: capability.YDB262()}, ast.StatementExtension, test.value)
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, []string{test.want})
			_, err = registry.Render(renderer.ExtensionContext{Target: "ydb"}, ast.StatementExtension, test.value)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			_, err = registry.Render(renderer.ExtensionContext{Target: "postgres", Capabilities: capability.YDB262()}, ast.StatementExtension, test.value)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		})
	}
}
