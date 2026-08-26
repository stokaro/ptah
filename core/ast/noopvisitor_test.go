package ast_test

import (
	"os"
	"reflect"
	"regexp"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
)

// tableRecorder embeds ast.NoopVisitor and overrides exactly one method. This is
// the shape NoopVisitor exists to support: a consumer that handles part of the
// AST and inherits a no-op answer for everything else.
type tableRecorder struct {
	ast.NoopVisitor

	tables []string
}

func (v *tableRecorder) VisitCreateTable(node *ast.CreateTableNode) error {
	v.tables = append(v.tables, node.Name)
	return nil
}

func TestNoopVisitorSatisfiesVisitor(t *testing.T) {
	c := qt.New(t)

	var byValue ast.Visitor = ast.NoopVisitor{}
	var byPointer ast.Visitor = &ast.NoopVisitor{}

	c.Assert(byValue, qt.IsNotNil)
	c.Assert(byPointer, qt.IsNotNil)
	c.Assert(reflect.TypeFor[ast.NoopVisitor]().NumMethod(), qt.Equals, visitorMethodCount())
}

func TestNoopVisitorIgnoresNodesItIsHanded(t *testing.T) {
	c := qt.New(t)

	visitor := ast.NoopVisitor{}

	c.Assert((&ast.CreateTableNode{Name: "users"}).Accept(visitor), qt.IsNil)
	c.Assert((&ast.DropTableNode{Name: "users"}).Accept(visitor), qt.IsNil)
}

func TestNoopVisitorEmbeddedOverrideWins(t *testing.T) {
	c := qt.New(t)

	visitor := &tableRecorder{}

	c.Assert((&ast.CreateTableNode{Name: "users"}).Accept(visitor), qt.IsNil)
	c.Assert((&ast.DropTableNode{Name: "users"}).Accept(visitor), qt.IsNil)

	// The override recorded the CREATE TABLE; the inherited no-op answered the
	// DROP TABLE without recording anything and without an error.
	c.Assert(visitor.tables, qt.DeepEquals, []string{"users"})
}

func TestPackageDocStatesTheVisitorMethodCount(t *testing.T) {
	c := qt.New(t)

	documented := documentedVisitorMethodCount(c)

	c.Assert(documented, qt.Equals, visitorMethodCount())
}

// visitorMethodCount is the number of methods ast.Visitor declares.
func visitorMethodCount() int {
	return reflect.TypeFor[ast.Visitor]().NumMethod()
}

// documentedVisitorMethodCount returns the method count this package's doc
// comment states for Visitor. The prose named eight methods while the interface
// had forty-six (stokaro/ptah#2246); reading the number back from the source
// binds the claim to the thing it describes.
func documentedVisitorMethodCount(c *qt.C) int {
	c.Helper()

	source, err := os.ReadFile("doc.go")
	c.Assert(err, qt.IsNil)

	match := regexp.MustCompile(`Visitor\] declares (\d+) of them`).FindSubmatch(source)
	c.Assert(match, qt.HasLen, 2)

	count, err := strconv.Atoi(string(match[1]))
	c.Assert(err, qt.IsNil)

	return count
}
