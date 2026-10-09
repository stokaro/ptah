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

func TestPrincipalEffectsDoNotOccupySchemePaths(t *testing.T) {
	for _, test := range []struct {
		name   string
		node   ast.Node
		action plangraph.Action
	}{
		{"create user", &ast.CreateRoleNode{Name: "App.Team"}, plangraph.Create},
		{"create group", &ast.CreateRoleNode{Name: "App.Team", Group: true}, plangraph.Create},
		{"alter user", &ast.AlterRoleNode{Name: "App.Team"}, plangraph.Alter},
		{"drop user", &ast.DropRoleNode{Name: "App.Team"}, plangraph.Drop},
		{"drop group", &ast.DropRoleNode{Name: "App.Team", Group: true}, plangraph.Drop},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			builder := objectidentity.NewBuilder(identifier.ForDialect("ydb"))
			effects, err := ydbscheme.CommonEffects(builder, test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(effects, qt.DeepEquals, []plangraph.Effect{{Subject: builder.Role("App.Team"), Action: test.action}})
			c.Assert(effects[0].Subject.Schema.Empty(), qt.IsTrue)
			c.Assert(effects[0].Subject.Name.Source, qt.Equals, "App.Team")
			c.Assert(effects[0].Subject.Name.Normalized, qt.Equals, "App.Team")
		})
	}
}

func TestPrincipalEffectsRefuseAnEmptyIdentity(t *testing.T) {
	for _, node := range []ast.Node{&ast.CreateRoleNode{}, &ast.AlterRoleNode{}, &ast.DropRoleNode{}} {
		c := qt.New(t)
		effects, err := ydbscheme.CommonEffects(objectidentity.NewBuilder(identifier.ForDialect("ydb")), node)
		c.Assert(err, qt.ErrorMatches, "YDB principal operation requires an object name")
		c.Assert(effects, qt.IsNil)
	}
}
