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

// TestVisitCreateFunction_LanguageDecidesWhetherTheBodyCanRun pins which
// declarations this target generates DDL for, and which it names and skips.
//
// MySQL and MariaDB run exactly one routine language, SQL. A function declared
// LANGUAGE plpgsql is PostgreSQL procedural code and no envelope makes it run
// here: the shared 014-rls-functions fixture declares
// `RETURNS VOID ... BEGIN PERFORM set_config(...); END;`, and rendering that as
// MySQL DDL reached Error 1064 on MySQL 26.7.0 at the return type, before the
// body was parsed.
//
// The `sql` and unset rows are the control, and they are the point. Skipping
// EVERY function would be `-- CREATE FUNCTION f1 not supported in MySQL` in a
// new spelling -- the false claim about the engine that stokaro/ptah#929 is
// about -- and it would make capability.Functions vacuous again. A body this
// target can run still becomes real DDL, which is what the live round trip in
// integration/gonative asserts.
func TestVisitCreateFunction_LanguageDecidesWhetherTheBodyCanRun(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		language string
		want     string
	}{
		{
			name: "sql is generated", language: "sql",
			want: "CREATE FUNCTION `fn`() RETURNS int DETERMINISTIC RETURN 1;",
		},
		{
			name: "unset is generated", language: "",
			want: "CREATE FUNCTION `fn`() RETURNS int DETERMINISTIC RETURN 1;",
		},
		{
			name: "uppercase SQL is generated", language: "SQL",
			want: "CREATE FUNCTION `fn`() RETURNS int DETERMINISTIC RETURN 1;",
		},
		{
			name: "plpgsql is named and skipped", language: "plpgsql",
			want: "CREATE FUNCTION `fn` declares language plpgsql, which this target does not run; skipped.",
		},
		{
			name: "plpython is named and skipped", language: "plpython3u",
			want: "CREATE FUNCTION `fn` declares language plpython3u, which this target does not run; skipped.",
		},
	}

	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			c.Run(dialect+"/"+test.name, func(c *qt.C) {
				r := newRenderer(dialect)

				err := r.VisitCreateFunction(&ast.CreateFunctionNode{
					Name: "fn", Returns: "int", Volatility: "IMMUTABLE",
					Language: test.language, Body: "RETURN 1",
				})

				c.Assert(err, qt.IsNil)
				c.Check(r.Output(), qt.Contains, test.want)
				// Whatever the answer, it never blames the engine.
				c.Check(r.Output(), qt.Not(qt.Contains), "not supported in")
			})
		}
	}
}

// TestVisitCreateFunction_SkippedLanguageEmitsNoStatement pins that the skip is
// a comment and nothing else.
//
// A skipped function must not leave the leading DROP behind: that would be an
// executable statement for an object this target never creates, and under
// `tx-mode file` the transaction witness guard would refuse the migration --
// which is how the six shared RLS fixtures went red.
func TestVisitCreateFunction_SkippedLanguageEmitsNoStatement(t *testing.T) {
	c := qt.New(t)

	for _, dialect := range []string{"mysql", "mariadb"} {
		c.Run(dialect, func(c *qt.C) {
			r := newRenderer(dialect)

			err := r.VisitCreateFunction(&ast.CreateFunctionNode{
				Name: "fn", Returns: "void", Language: "plpgsql",
				Body: "BEGIN PERFORM set_config('a', 'b', false); END;",
			})

			c.Assert(err, qt.IsNil)
			c.Check(r.Output(), qt.Not(qt.Contains), "DROP FUNCTION")
			c.Check(r.Output(), qt.Not(qt.Contains), "CREATE FUNCTION `fn`(")
		})
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
