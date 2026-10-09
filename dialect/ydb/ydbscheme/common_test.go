package ydbscheme_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/dialect/ydb/ydbscheme"
)

func TestCommonEffectsUseRenderedSchemePaths(t *testing.T) {
	for _, test := range []struct {
		name string
		path string
		ref  objectidentity.ID
	}{
		{name: "app.locks", path: "app/locks", ref: ydbscheme.Path("app", "locks")},
		{name: `"app"."locks"`, path: "app/locks", ref: ydbscheme.Path("app", "locks")},
		{name: "`app/locks`", path: "app/locks", ref: ydbscheme.Path("app", "locks")},
		{name: "`app.locks`", path: "app.locks", ref: ydbscheme.Path("", "app.locks")},
		{name: "`app/locks.v1`", path: "app/locks.v1", ref: ydbscheme.Path("app", "locks.v1")},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			builder := objectidentity.NewBuilder(identifier.ForDialect("ydb"))
			c.Assert(ydbscheme.ObjectPath(test.name), qt.Equals, test.path)
			view, err := ydbscheme.CommonEffects(builder, &ast.CreateViewNode{Name: test.name})
			c.Assert(err, qt.IsNil)
			c.Assert(view, qt.DeepEquals, []plangraph.Effect{{Subject: test.ref, Action: plangraph.Create}})
			table, err := ydbscheme.CommonEffects(builder, &ast.DropTableNode{Name: test.name})
			c.Assert(err, qt.IsNil)
			c.Assert(table, qt.DeepEquals, []plangraph.Effect{
				{Subject: test.ref, Action: plangraph.Drop},
				{Subject: builder.Table(test.name), Action: plangraph.Drop},
			})
		})
	}
}
