package fromschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/internal/convert/fromschema"
)

func TestFromSequence(t *testing.T) {
	c := qt.New(t)

	node := fromschema.FromSequence(goschema.Sequence{
		Name:        "order_seq",
		Schema:      "app",
		AsType:      "bigint",
		Start:       new(int64(1000)),
		Increment:   new(int64(2)),
		Cache:       new(int64(20)),
		Cycle:       true,
		OwnedBy:     "orders.id",
		IfNotExists: true,
		Comment:     "Order numbers",
	})

	c.Assert(node.Name, qt.Equals, "order_seq")
	c.Assert(node.Schema, qt.Equals, "app")
	c.Assert(node.AsType, qt.Equals, "bigint")
	c.Assert(*node.Start, qt.Equals, int64(1000))
	c.Assert(*node.Increment, qt.Equals, int64(2))
	c.Assert(*node.Cache, qt.Equals, int64(20))
	c.Assert(node.Cycle, qt.IsTrue)
	c.Assert(node.OwnedBy, qt.Equals, "orders.id")
	c.Assert(node.IfNotExists, qt.IsTrue)
	c.Assert(node.Comment, qt.Equals, "Order numbers")
}

// TestFromDatabase_SequenceOrdering asserts a standalone sequence is created
// before tables (so a column DEFAULT can reference it) while its OWNED BY
// association is emitted after tables (which require the owning column to
// exist).
func TestFromDatabase_SequenceOrdering(t *testing.T) {
	c := qt.New(t)

	database := goschema.Database{
		Sequences: []goschema.Sequence{
			{Name: "order_seq", OwnedBy: "orders.id"},
		},
		Tables: []goschema.Table{
			{StructName: "Order", Name: "orders"},
		},
		Fields: []goschema.Field{
			{StructName: "Order", Name: "id", Type: "BIGINT", Primary: true},
		},
	}

	statements := fromschema.FromDatabase(database, platform.Postgres)

	createNode := createSequenceStatementByName(statements, "order_seq")
	createIdx := createSequenceStatementIndexByName(statements, "order_seq")
	ownedIdx := alterSequenceStatementIndexByName(statements, "order_seq")
	tableIdx := tableStatementIndexByName(statements, "orders")

	c.Assert(createNode, qt.IsNotNil, qt.Commentf("CREATE SEQUENCE must be emitted"))
	c.Assert(createNode.OwnedBy, qt.Equals, "", qt.Commentf("CREATE SEQUENCE must not carry inline OWNED BY"))
	c.Assert(tableIdx >= 0, qt.IsTrue, qt.Commentf("CREATE TABLE must be emitted"))
	c.Assert(ownedIdx >= 0, qt.IsTrue, qt.Commentf("ALTER SEQUENCE OWNED BY must be emitted"))
	c.Assert(createIdx < tableIdx, qt.IsTrue, qt.Commentf("CREATE SEQUENCE must precede CREATE TABLE"))
	c.Assert(tableIdx < ownedIdx, qt.IsTrue, qt.Commentf("OWNED BY must follow CREATE TABLE"))
}

func TestFromGrant_OnSequence(t *testing.T) {
	c := qt.New(t)

	node := fromschema.FromGrant(goschema.Grant{
		Role:       "app_user",
		Privileges: []string{"USAGE", "SELECT"},
		OnSequence: "order_seq",
	})

	c.Assert(node.ObjectType, qt.Equals, "SEQUENCE")
	c.Assert(node.ObjectName, qt.Equals, "order_seq")
}

// TestFromDatabase_SequenceSkippedForMySQL confirms standalone sequences are not
// emitted for non-PostgreSQL targets (they would render to no executable DDL).
func TestFromDatabase_SequenceSkippedForMySQL(t *testing.T) {
	c := qt.New(t)

	database := goschema.Database{
		Sequences: []goschema.Sequence{{Name: "order_seq"}},
	}

	statements := fromschema.FromDatabase(database, platform.MySQL)

	for _, stmt := range statements.Statements {
		_, isCreate := stmt.(*ast.CreateSequenceNode)
		_, isAlter := stmt.(*ast.AlterSequenceNode)
		c.Assert(isCreate || isAlter, qt.IsFalse, qt.Commentf("no sequence nodes for MySQL"))
	}
}
