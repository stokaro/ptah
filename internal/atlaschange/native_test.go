package atlaschange_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
	"ptah.run/internal/atlaschange"
)

// plannedOperation is an operation a provider planned, as the namer meets it
// in a plan: the object it is planned for and its step's effects.
type plannedOperation struct {
	subject objectidentity.ID
	effects []plangraph.Effect
}

func (o *plannedOperation) Kind() schemaext.Kind        { return "example.org/provider/operation" }
func (o *plannedOperation) Subject() objectidentity.ID  { return o.subject }
func (o *plannedOperation) Effects() []plangraph.Effect { return slices.Clone(o.effects) }
func (o *plannedOperation) CloneExtension() ast.ExtensionPayload {
	return &plannedOperation{o.subject, slices.Clone(o.effects)}
}

// opaqueOperation is an owner operation that reports nothing about the change
// it makes, such as a built-in feature's.
type opaqueOperation struct{}

func (opaqueOperation) Kind() schemaext.Kind                 { return "example.org/feature/operation" }
func (opaqueOperation) CloneExtension() ast.ExtensionPayload { return opaqueOperation{} }

// part is one identity component spelled as written.
func part(name string) objectidentity.Part {
	return objectidentity.Part{Source: name, Normalized: name}
}

// object is the identity of an object of kind named name in schema app.
func object(kind objectidentity.Kind, name string) objectidentity.ID {
	return objectidentity.ID{Kind: kind, Schema: part("app"), Name: part(name)}
}

// planned is the statement of an operation planned for subject whose step has
// effects.
func planned(subject objectidentity.ID, effects ...plangraph.Effect) ast.Node {
	return &ast.ExtensionStatement{Payload: &plannedOperation{subject: subject, effects: effects}}
}

// An operation a provider planned is named in the wording of the same change's
// statement, from its subject and its step's effect on that subject, or for a
// step that only adds or drops one index of the table it reads, from that
// index, and the comment a rollback statement of that kind carries is the
// forward change it undoes.
func TestOfAndUndoneOfAProviderOperation_HappyPath(t *testing.T) {
	orders := object(objectidentity.KindTable, "orders")
	index := objectidentity.ID{Kind: objectidentity.KindIndex, Schema: part("app"), Parent: part("orders"), Name: part("orders_by_total")}
	other := objectidentity.ID{Kind: objectidentity.KindIndex, Schema: part("app"), Parent: part("orders"), Name: part("orders_by_note")}
	elsewhere := objectidentity.ID{Kind: objectidentity.KindIndex, Schema: part("app"), Parent: part("lines"), Name: part("lines_by_qty")}
	tests := []struct {
		name   string
		node   ast.Node
		of     string
		undone string
	}{
		{
			name:   "a table the step creates, named without its schema",
			node:   planned(orders, plangraph.Effect{Subject: orders, Action: plangraph.Create}),
			of:     `create "orders" table`,
			undone: `drop "orders" table`,
		},
		{
			name:   "a table the step drops",
			node:   planned(orders, plangraph.Effect{Subject: orders, Action: plangraph.Drop}),
			of:     `drop "orders" table`,
			undone: `create "orders" table`,
		},
		{
			name:   "a table the step alters",
			node:   planned(orders, plangraph.Effect{Subject: orders, Action: plangraph.Alter}),
			of:     `modify "orders" table`,
			undone: `modify "orders" table`,
		},
		{
			name:   "an index the step adds to a table it reads, named as the built-in YDB engine named it",
			node:   planned(orders, plangraph.Effect{Subject: orders, Action: plangraph.Read}, plangraph.Effect{Subject: index, Action: plangraph.Create}),
			of:     `create index "orders_by_total" to table: "orders"`,
			undone: `drop index "orders_by_total" from table: "orders"`,
		},
		{
			name:   "an index the step drops from a table it reads",
			node:   planned(orders, plangraph.Effect{Subject: orders, Action: plangraph.Read}, plangraph.Effect{Subject: index, Action: plangraph.Drop}),
			of:     `drop index "orders_by_total" from table: "orders"`,
			undone: `create index "orders_by_total" to table: "orders"`,
		},
		{
			name:   "an index the step changes on a table it reads",
			node:   planned(orders, plangraph.Effect{Subject: orders, Action: plangraph.Read}, plangraph.Effect{Subject: index, Action: plangraph.Alter}),
			of:     `modify "orders" table`,
			undone: `modify "orders" table`,
		},
		{
			name: "an index the step adds to a table it also alters",
			node: planned(orders, plangraph.Effect{Subject: orders, Action: plangraph.Alter},
				plangraph.Effect{Subject: index, Action: plangraph.Create}),
			of:     `modify "orders" table`,
			undone: `modify "orders" table`,
		},
		{
			name: "two indexes the step adds",
			node: planned(orders, plangraph.Effect{Subject: orders, Action: plangraph.Read}, plangraph.Effect{Subject: index, Action: plangraph.Create},
				plangraph.Effect{Subject: other, Action: plangraph.Create}),
			of:     `modify "orders" table`,
			undone: `modify "orders" table`,
		},
		{
			name:   "an index of another table",
			node:   planned(orders, plangraph.Effect{Subject: orders, Action: plangraph.Read}, plangraph.Effect{Subject: elsewhere, Action: plangraph.Create}),
			of:     `modify "orders" table`,
			undone: `modify "orders" table`,
		},
		{
			name:   "an index the step creates as its own subject",
			node:   planned(index, plangraph.Effect{Subject: index, Action: plangraph.Create}),
			of:     `create index "orders_by_total" to table: "orders"`,
			undone: `drop index "orders_by_total" from table: "orders"`,
		},
		{
			name:   "a view the step creates",
			node:   planned(object(objectidentity.KindView, "totals"), plangraph.Effect{Subject: object(objectidentity.KindView, "totals"), Action: plangraph.Create}),
			of:     `create "totals" view`,
			undone: `drop "totals" view`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			of, ok := nameOf(test.node)

			c.Assert(ok, qt.IsTrue)
			c.Assert(of, qt.Equals, test.of)
			undone, ok := nameUndone(test.node)
			c.Assert(ok, qt.IsTrue)
			c.Assert(undone, qt.Equals, test.undone)
		})
	}
}

// An owner operation gets no comment where the namer cannot say what it does:
// it is planned for an object of a kind of the provider's own, which the
// community binary names nothing of, it carries no effects, as an operation
// decoded without its plan step does, or it reports neither its subject nor
// its effects.
func TestOfAndUndoneOfAProviderOperation_FailurePath(t *testing.T) {
	topic := objectidentity.ID{Kind: "topic", Schema: part("app"), Name: part("events")}
	orders := object(objectidentity.KindTable, "orders")
	tests := []struct {
		name string
		node ast.Node
	}{
		{name: "an object of the provider's own kind", node: planned(topic, plangraph.Effect{Subject: topic, Action: plangraph.Create})},
		{name: "an operation without effects", node: planned(orders)},
		{name: "an operation that reports neither", node: &ast.ExtensionStatement{Payload: opaqueOperation{}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			of, ok := nameOf(test.node)

			c.Assert(ok, qt.IsFalse)
			c.Assert(of, qt.Equals, "")
			undone, ok := nameUndone(test.node)
			c.Assert(ok, qt.IsFalse)
			c.Assert(undone, qt.Equals, "")
		})
	}
}

// A rollback drop of a table the forward plan's provider operation only
// altered undoes that alteration, and a drop of the table of the same name in
// another schema undoes a creation, as for a planned statement.
func TestRollbackOfAProviderOperation_HappyPath(t *testing.T) {
	orders := object(objectidentity.KindTable, "orders")
	elsewhere := objectidentity.ID{Kind: objectidentity.KindTable, Schema: part("reporting"), Name: part("orders")}
	forward := []ast.Node{planned(orders, plangraph.Effect{Subject: orders, Action: plangraph.Alter})}
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{name: "a drop of the table the forward plan altered", node: planned(orders, plangraph.Effect{Subject: orders, Action: plangraph.Drop}), want: `modify "orders" table`},
		{name: "a drop of the table of the same name in another schema", node: planned(elsewhere, plangraph.Effect{Subject: elsewhere, Action: plangraph.Drop}), want: `create "orders" table`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, ok := nameRollback(forward, test.node)

			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// effect is the effect action has on subject.
func effect(subject objectidentity.ID, action plangraph.Action) plangraph.Effect {
	return plangraph.Effect{Subject: subject, Action: action}
}

// providerRebuild is the plan a provider writes to rebuild table orders with
// its changefeed and the held settings of its index orders_by_total, step by
// step as the YDB provider plans it: the new table, its index's settings, the
// copy, the changefeed's drop, the two moves, the changefeed again and the drop
// of the old table. The new table is created before an unrelated table, which
// keeps its name.
func providerRebuild() []ast.Node {
	orders := object(objectidentity.KindTable, "orders")
	scratch := object(objectidentity.KindTable, "__ptah_rebuild_orders")
	replaced := object(objectidentity.KindTable, "__ptah_replaced_orders")
	lines := object(objectidentity.KindTable, "lines")
	slot := func(table objectidentity.ID) objectidentity.ID {
		table.Kind = "ydb.scheme-path"
		return table
	}
	index := objectidentity.ID{Kind: objectidentity.KindIndex, Schema: part("app"), Parent: part("__ptah_rebuild_orders"), Name: part("orders_by_total")}
	feed := objectidentity.ID{Kind: "ydb.changefeed", Schema: part("app"), Parent: part("orders"), Name: part("feed")}
	return []ast.Node{
		planned(scratch, effect(scratch, plangraph.Create), effect(slot(scratch), plangraph.Create)),
		planned(lines, effect(lines, plangraph.Create)),
		planned(scratch, effect(scratch, plangraph.Read), effect(index, plangraph.Alter)),
		planned(scratch, effect(orders, plangraph.Read), effect(scratch, plangraph.Alter)),
		planned(orders, effect(orders, plangraph.Read), effect(feed, plangraph.Drop)),
		planned(orders, effect(orders, plangraph.Alter), effect(replaced, plangraph.Create), effect(slot(replaced), plangraph.Create)),
		planned(scratch, effect(scratch, plangraph.Drop), effect(slot(scratch), plangraph.Drop), effect(orders, plangraph.Alter)),
		planned(orders, effect(orders, plangraph.Read), effect(feed, plangraph.Create)),
		planned(replaced, effect(replaced, plangraph.Drop), effect(slot(replaced), plangraph.Drop)),
	}
}

// A table rebuild a provider planned is named as the built-in YDB engine named
// its own: the new table's creation, each change to the table it replaces, and
// the old table's drop. The steps that fill the new table and the two moves
// take no name, so the move of the new table onto the old one's name does not
// read as a drop of the new table. A rollback that rebuilds a table names the
// same steps the change each one undoes.
func TestATableRebuildAProviderPlanned_HappyPath(t *testing.T) {
	c := qt.New(t)

	forward := atlaschange.Words{}.Forward(providerRebuild())
	rollback := atlaschange.Words{}.Rollback(nil, providerRebuild())

	c.Assert(forward, qt.DeepEquals, []string{
		`create "__ptah_rebuild_orders" table`,
		`create "lines" table`,
		"",
		"",
		`modify "orders" table`,
		"",
		"",
		`modify "orders" table`,
		`drop "__ptah_replaced_orders" table`,
	})
	c.Assert(rollback, qt.DeepEquals, []string{
		`drop "__ptah_rebuild_orders" table`,
		`drop "lines" table`,
		"",
		"",
		`modify "orders" table`,
		"",
		"",
		`modify "orders" table`,
		`create "__ptah_replaced_orders" table`,
	})
}

// A step on a new table is part of a rebuild only between the table's creation
// and its move onto another table's name. Without the move, the copy reads as
// a change of the table it alters. Without the creation, the table is not one
// the plan made, and neither is a change after the move. A step that alters
// two tables, or creates two, is no move.
func TestATableRebuildAProviderPlanned_FailurePath(t *testing.T) {
	orders := object(objectidentity.KindTable, "orders")
	scratch := object(objectidentity.KindTable, "__ptah_rebuild_orders")
	other := object(objectidentity.KindTable, "other")
	tests := []struct {
		name string
		plan []ast.Node
		want []string
	}{
		{
			name: "a copy without a move",
			plan: []ast.Node{
				planned(scratch, effect(scratch, plangraph.Create)),
				planned(scratch, effect(orders, plangraph.Read), effect(scratch, plangraph.Alter)),
			},
			want: []string{`create "__ptah_rebuild_orders" table`, `modify "__ptah_rebuild_orders" table`},
		},
		{
			name: "a copy into a table no step creates",
			plan: []ast.Node{
				planned(scratch, effect(orders, plangraph.Read), effect(scratch, plangraph.Alter)),
				planned(scratch, effect(scratch, plangraph.Drop), effect(orders, plangraph.Alter)),
			},
			want: []string{`modify "__ptah_rebuild_orders" table`, ""},
		},
		{
			name: "a change after the move",
			plan: []ast.Node{
				planned(scratch, effect(scratch, plangraph.Create)),
				planned(scratch, effect(scratch, plangraph.Drop), effect(orders, plangraph.Alter)),
				planned(scratch, effect(orders, plangraph.Read), effect(scratch, plangraph.Alter)),
			},
			want: []string{`create "__ptah_rebuild_orders" table`, "", `modify "__ptah_rebuild_orders" table`},
		},
		{
			name: "a step that alters two tables",
			plan: []ast.Node{planned(orders, effect(orders, plangraph.Alter), effect(other, plangraph.Alter))},
			want: []string{`modify "orders" table`},
		},
		{
			name: "a step that creates two tables",
			plan: []ast.Node{planned(orders, effect(orders, plangraph.Create), effect(other, plangraph.Create))},
			want: []string{`create "orders" table`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := atlaschange.Words{}.Forward(test.plan)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
