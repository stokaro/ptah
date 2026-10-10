package pgeffects_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/pgeffects"
)

var builder = objectidentity.NewBuilder(identifier.ForDialect("postgres"))

func effect(subject objectidentity.ID, action plangraph.Action) plangraph.Effect {
	return plangraph.Effect{Subject: subject, Action: action}
}

func view(kind objectidentity.Kind, name string) objectidentity.ID {
	ref := builder.Table(name)
	ref.Kind = kind
	return ref
}

// TestSequence_ReadsEachStatementInItsHistory pins the effects a sequence of
// common statements declares: a second write to an object the sequence created
// is a change, a write after a drop keeps no footprint, and a statement this
// package does not know declares none.
func TestSequence_ReadsEachStatementInItsHistory(t *testing.T) {
	c := qt.New(t)
	nodes := []ast.Node{
		&ast.CreateTableNode{Name: "readings"},
		&ast.AlterTableNode{Name: "readings", Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: &ast.ColumnNode{Name: "time"}}}},
		&ast.CreateViewNode{Name: "recent"},
		&ast.CreateViewNode{Name: "recent", Replace: true},
		&ast.CreateMaterializedViewNode{Name: "daily"},
		&ast.DropTableNode{Name: "readings"},
		&ast.AlterTableNode{Name: "readings"},
		&ast.IndexNode{Name: "readings_time_idx", Table: "readings"},
		&ast.CreateTableNode{Name: ""},
	}

	effects := pgeffects.Sequence(builder, nodes)

	c.Assert(effects, qt.DeepEquals, [][]plangraph.Effect{
		{effect(builder.Table("readings"), plangraph.Create)},
		{effect(builder.Table("readings"), plangraph.Alter), effect(builder.Column("readings", "time"), plangraph.Create)},
		{effect(view(objectidentity.KindView, "recent"), plangraph.Create)},
		{effect(view(objectidentity.KindView, "recent"), plangraph.Alter)},
		{effect(view(objectidentity.KindMatView, "daily"), plangraph.Create)},
		{effect(builder.Table("readings"), plangraph.Drop)},
		{},
		nil,
		nil,
	})
	c.Assert(pgeffects.CreatesView(effects[2]), qt.IsTrue)
	c.Assert(pgeffects.CreatesView(effects[4]), qt.IsTrue)
	c.Assert(pgeffects.CreatesView(effects[3]), qt.IsFalse)
	c.Assert(pgeffects.CreatesView(effects[0]), qt.IsFalse)
}

// TestStatement_ReadsColumnDropsRoutinesAndRoles pins the effects an owner
// orders a dependent object against: the column an ALTER TABLE drops, a routine
// created, replaced or dropped, and a role created, changed or dropped. A
// routine is identified by its input argument types, so a creation that names
// its parameters, gives one a default and declares an OUT argument, and the
// catalog's identity a drop names, are one routine. A procedure is not a
// function.
func TestStatement_ReadsColumnDropsRoutinesAndRoles(t *testing.T) {
	arguments, catalogIdentity, none := "integer", "n character varying", ""
	tests := []struct {
		name string
		node ast.Node
		want []plangraph.Effect
	}{
		{name: "a dropped column", node: &ast.AlterTableNode{Name: "orders", Operations: []ast.AlterOperation{&ast.DropColumnOperation{ColumnName: "tenant"}}},
			want: []plangraph.Effect{effect(builder.Table("orders"), plangraph.Alter), effect(builder.Column("orders", "tenant"), plangraph.Drop)}},
		{name: "a function created", node: &ast.CreateFunctionNode{Name: "app.tenant", Parameters: "integer"},
			want: []plangraph.Effect{effect(builder.Function("app.tenant", "integer"), plangraph.Create)}},
		{name: "a procedure created", node: &ast.CreateFunctionNode{Name: "app.refresh", Kind: "procedure"},
			want: []plangraph.Effect{effect(routineOf(objectidentity.KindProcedure, "app.refresh", ""), plangraph.Create)}},
		{name: "a function dropped with its arguments", node: &ast.DropFunctionNode{Name: "app.tenant", Parameters: &arguments},
			want: []plangraph.Effect{effect(builder.Function("app.tenant", "integer"), plangraph.Drop)}},
		{name: "a function created with names, a default, a modifier and an OUT argument",
			node: &ast.CreateFunctionNode{Name: "app.tenant", Parameters: "n varchar(20) DEFAULT 'x', OUT r int"},
			want: []plangraph.Effect{effect(builder.Function("app.tenant", "character varying"), plangraph.Create)}},
		{name: "the same function dropped by the catalog's identity", node: &ast.DropFunctionNode{Name: "app.tenant", Parameters: &catalogIdentity},
			want: []plangraph.Effect{effect(builder.Function("app.tenant", "character varying"), plangraph.Drop)}},
		{name: "a function replaced", node: &ast.CreateFunctionNode{Name: "app.tenant", Parameters: "integer", Replace: true},
			want: []plangraph.Effect{effect(builder.Function("app.tenant", "integer"), plangraph.Alter)}},
		{name: "the zero-argument function dropped", node: &ast.DropFunctionNode{Name: "app.tenant", Parameters: &none},
			want: []plangraph.Effect{effect(builder.Function("app.tenant", ""), plangraph.Drop)}},
		{name: "a function dropped by name alone", node: &ast.DropFunctionNode{Name: "app.tenant"}},
		{name: "a role created", node: &ast.CreateRoleNode{Name: "reader"}, want: []plangraph.Effect{effect(builder.Role("reader"), plangraph.Create)}},
		{name: "a role changed", node: &ast.AlterRoleNode{Name: "reader"}, want: []plangraph.Effect{effect(builder.Role("reader"), plangraph.Alter)}},
		{name: "a role dropped", node: &ast.DropRoleNode{Name: "reader"}, want: []plangraph.Effect{effect(builder.Role("reader"), plangraph.Drop)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgeffects.Statement(builder, test.node), qt.DeepEquals, test.want)
		})
	}
}

func routineOf(kind objectidentity.Kind, name, signature string) objectidentity.ID {
	ref := builder.Function(name, signature)
	ref.Kind = kind
	return ref
}
