package ast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
)

// TestCreateTableNode_ElementsRecordEveryAdditionInCallOrder pins the contract
// stokaro/ptah#2773 added to this node.
//
// Constraints and Indexes are two slices, so between them they cannot say which
// element came first, and on MySQL and MariaDB that decides which index names
// exist. Elements is where the interleaving lives, and it is a promise to
// whoever reads it: an element points at the very node the kind's own slice
// holds, so a reader can walk the order and write to what it finds.
//
// The pointers are asserted rather than the values, because an equal copy would
// satisfy a comparison and satisfy no caller: the pass that reads the order
// names an index by assigning to it.
func TestCreateTableNode_ElementsRecordEveryAdditionInCallOrder(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable("users")
	unique := ast.NewUniqueConstraint("uk_users_email", "email")
	first := ast.NewIndex("idx_users_name", "users", "name")
	foreignKey := ast.NewForeignKeyConstraint("fk_users_org", []string{"org_id"},
		&ast.ForeignKeyRef{Table: "orgs", Column: "id"})
	second := ast.NewIndex("idx_users_city", "users", "city")

	table.AddConstraint(unique).AddIndex(first).
		AddConstraint(foreignKey).AddIndex(second)

	c.Assert(table.Elements, qt.HasLen, 4)
	c.Assert(table.Elements[0].Constraint, qt.Equals, unique)
	c.Assert(table.Elements[0].Index, qt.IsNil)
	c.Assert(table.Elements[1].Index, qt.Equals, first)
	c.Assert(table.Elements[1].Constraint, qt.IsNil)
	c.Assert(table.Elements[2].Constraint, qt.Equals, foreignKey)
	c.Assert(table.Elements[3].Index, qt.Equals, second)

	// The two slices still hold what they always did, in their own order: a
	// reader that has no use for the interleaving is unaffected by its being
	// recorded.
	c.Assert(table.Constraints, qt.DeepEquals, []*ast.ConstraintNode{unique, foreignKey})
	c.Assert(table.Indexes, qt.DeepEquals, []*ast.IndexNode{first, second})
}

// TestCreateTableNode_ANodeNothingOrderedRecordsNoElements is the other half of
// the same contract, and the one a reader has to handle.
//
// A node assembled field by field carries no order, and Elements answers that
// honestly rather than inventing one. That empty slice is what tells a reader to
// fall back to constraints before indexes; a node that guessed an order would
// look like a document that had declared it.
func TestCreateTableNode_ANodeNothingOrderedRecordsNoElements(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable("users")
	table.Constraints = []*ast.ConstraintNode{ast.NewUniqueConstraint("", "email")}
	table.Indexes = []*ast.IndexNode{ast.NewIndex("", "users", "name")}

	c.Assert(table.Elements, qt.HasLen, 0)
}
