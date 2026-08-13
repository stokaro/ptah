package mysqllike_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/internal/bufwriter"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/mysqllike"
)

// newRenderer builds the shared renderer for one member of the family.
func newRenderer(dialect string) *mysqllike.Renderer {
	return mysqllike.NewWithCapabilities(dialect, &bufwriter.Writer{}, capability.ForDialect(dialect))
}

// TestVisitDropFunction_QualifiedNameIsTwoIdentifiers pins the quoting of a
// schema-qualified function name.
//
// A function read back from the catalog carries its database as a schema, and
// the diff names removals by DBFunction.QualifiedName(), so the drop path is
// handed `ptah_test.f_c` rather than `f_c`. Quoting that whole string as one
// identifier yields
//
//	DROP FUNCTION IF EXISTS `ptah_test.f_c`
//
// which names a function whose name literally contains a dot. The real routine
// is never dropped, so the diff re-plans the same drop forever.
//
// The defect was reachable only once the reader started returning functions:
// before that FunctionsRemoved was permanently empty on these targets and no
// drop was ever planned. Live against MySQL 26.7.0, the one-identifier form
// left two stray routines in information_schema.ROUTINES across an apply, and
// the two-identifier form dropped them and converged.
//
// VisitDropView already quotes this way; this holds the function path to the
// same rule rather than fixing the instance.
func TestVisitDropFunction_QualifiedNameIsTwoIdentifiers(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		node *ast.DropFunctionNode
		want string
	}{
		{
			name: "qualified",
			node: &ast.DropFunctionNode{Name: "ptah_test.f_c", IfExists: true},
			want: "DROP FUNCTION IF EXISTS `ptah_test`.`f_c`;",
		},
		{
			name: "bare name is untouched",
			node: &ast.DropFunctionNode{Name: "f_c", IfExists: true},
			want: "DROP FUNCTION IF EXISTS `f_c`;",
		},
	}

	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			c.Run(dialect+"/"+test.name, func(c *qt.C) {
				r := newRenderer(dialect)

				c.Assert(r.VisitDropFunction(test.node), qt.IsNil)
				c.Check(r.Output(), qt.Contains, test.want)
			})
		}
	}
}

// TestVisitCreateFunction_QualifiedNameIsTwoIdentifiers holds the create half to
// the same rule. Its leading DROP is generated from the same name, so a
// one-identifier bug here drops the wrong routine before creating the right
// one.
func TestVisitCreateFunction_QualifiedNameIsTwoIdentifiers(t *testing.T) {
	c := qt.New(t)

	for _, dialect := range []string{"mysql", "mariadb"} {
		c.Run(dialect, func(c *qt.C) {
			r := newRenderer(dialect)

			err := r.VisitCreateFunction(&ast.CreateFunctionNode{
				Name: "ptah_test.f_c", Returns: "int", Volatility: "IMMUTABLE", Body: "RETURN 1",
			})

			c.Assert(err, qt.IsNil)
			c.Check(r.Output(), qt.Contains, "DROP FUNCTION IF EXISTS `ptah_test`.`f_c`;")
			c.Check(r.Output(), qt.Contains, "CREATE FUNCTION `ptah_test`.`f_c`() RETURNS int DETERMINISTIC RETURN 1;")
			c.Check(r.Output(), qt.Not(qt.Contains), "`ptah_test.f_c`")
		})
	}
}
