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
