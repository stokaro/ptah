package fromschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/convert/fromschema"
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

// TestFromDatabase_SequenceReachesTheMySQLRendererToBeRefused pins that a
// declared sequence is handed to the MySQL-family renderer rather than dropped
// here.
//
// It used to be dropped, so `ptah schema render --dialect mariadb` omitted a
// declared sequence with no statement and no diagnostic while
// capability.MariaDB1011 advertised Sequences: true (stokaro/ptah#931 item 8).
// The renderer is what decides the target cannot host one, and it says so.
func TestFromDatabase_SequenceReachesTheMySQLRendererToBeRefused(t *testing.T) {
	c := qt.New(t)

	database := goschema.Database{
		Sequences: []goschema.Sequence{{Name: "order_seq"}},
	}

	statements := fromschema.FromDatabase(database, platform.MySQL)

	c.Assert(countCreateSequenceNodes(statements.Statements), qt.Equals, 1,
		qt.Commentf("the declared sequence must reach the renderer"))
}

func countCreateSequenceNodes(statements []ast.Node) int {
	created := 0
	for _, statement := range statements {
		if _, isCreate := statement.(*ast.CreateSequenceNode); isCreate {
			created++
		}
	}
	return created
}

// TestFromDatabase_SequenceReachesEveryDialectsRenderer states the rule this
// converter now follows, over every dialect spelling `--dialect` accepts: a
// declared sequence becomes exactly one CREATE SEQUENCE node whatever the
// target is, and the renderer decides what that means.
//
// This replaces a test that asserted the OPPOSITE for SQLite -- that no sequence
// node was produced there -- which is how the defect stayed pinned. SQLite and
// SQL Server were held out of the append because the SQL Server renderer
// answered a sequence node with a flat "CREATE SEQUENCE is not supported", a
// false statement about an engine that has had sequences since 2012. That
// message now names Ptah's generator instead of the engine, so there is nothing
// left to hold out (stokaro/ptah#929 item 5).
func TestFromDatabase_SequenceReachesEveryDialectsRenderer(t *testing.T) {
	c := qt.New(t)

	for _, spelling := range acceptedSpellings(c.TB) {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)
			database := goschema.Database{
				Sequences: []goschema.Sequence{{Name: "order_seq"}},
			}

			statements := fromschema.FromDatabase(database, spelling)

			c.Assert(countCreateSequenceNodes(statements.Statements), qt.Equals, 1,
				qt.Commentf("the declared sequence must reach the %s renderer", spelling))
		})
	}
}
