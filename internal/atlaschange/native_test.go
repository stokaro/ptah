package atlaschange_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/schemaext"
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
// statement, from its subject and its step's effect on that subject, and the
// comment a rollback statement of that kind carries is the forward change it
// undoes.
func TestOfAndUndoneOfAProviderOperation_HappyPath(t *testing.T) {
	orders := object(objectidentity.KindTable, "orders")
	index := objectidentity.ID{Kind: objectidentity.KindIndex, Schema: part("app"), Parent: part("orders"), Name: part("orders_by_total")}
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
			name:   "an index the step adds to a table it reads",
			node:   planned(orders, plangraph.Effect{Subject: orders, Action: plangraph.Read}, plangraph.Effect{Subject: index, Action: plangraph.Create}),
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
