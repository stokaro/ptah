package mysqllike_test

import (
	"strings"
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
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
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
// It is a SKIP and not a refusal, deliberately. A refusal breaks applying one
// schema across postgres, mysql and mariadb, and Ptah has no way to scope a
// declared object to a dialect: `//ptah:schema:function` takes no `platform=`
// or `dialect=` attribute, and unknown attributes are a hard parse error. Until
// a declaration can say "this object is PostgreSQL's", the skip is what lets a
// multi-dialect schema exist at all.
//
// The `sql` and unset rows are the control, and they are the point. Skipping
// EVERY function would be `-- CREATE FUNCTION f1 not supported in MySQL` in a
// new spelling -- the false claim about the engine that stokaro/ptah#929 is
// about -- and it would make capability.Functions vacuous again. A body this
// target can run still becomes real DDL, which is what the live round trip in
// integration/gonative asserts.
func TestVisitCreateFunction_LanguageDecidesWhetherTheBodyCanRun(t *testing.T) {
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
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
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

// TestVisitCreateFunction_SkipNamesTheCanonicalizeDefault pins the part of the
// skip message that is new.
//
// [goschema.Function.Canonicalize] defaults an UNSET language to plpgsql, so a
// function annotated without `language=` reaches the skip branch and is passed
// over when it should have been generated. Measured on MySQL 26.7.0 and MariaDB
// 12.3.2, `schema apply` then exits 0 having created nothing and the diff asks
// for the same function on every run. The renderer cannot tell that case apart
// from a deliberate plpgsql declaration -- both arrive as the same value -- so
// it cannot choose for the operator, but it can say which two readings exist
// and which word settles it.
func TestVisitCreateFunction_SkipNamesTheCanonicalizeDefault(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			r := newRenderer(dialect)

			err := r.VisitCreateFunction(&ast.CreateFunctionNode{
				Name: "fn", Returns: "int", Volatility: "IMMUTABLE",
				Language: "plpgsql", Body: "RETURN 1",
			})

			c.Assert(err, qt.IsNil)
			c.Check(r.Output(), qt.Contains, `language="sql"`)
			c.Check(r.Output(), qt.Contains, "defaulted to plpgsql")
			// Every added line is a comment: a skipped function must leave no
			// executable statement behind.
			for line := range strings.SplitSeq(strings.TrimSpace(r.Output()), "\n") {
				c.Check(strings.HasPrefix(strings.TrimSpace(line), "--"), qt.IsTrue,
					qt.Commentf("line is not a comment: %s", line))
			}
		})
	}
}

// TestVisitCreateFunction_QualifiedNameIsTwoIdentifiers holds the create half to
// the same rule as the drop half above.
func TestVisitCreateFunction_QualifiedNameIsTwoIdentifiers(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			r := newRenderer(dialect)

			err := r.VisitCreateFunction(&ast.CreateFunctionNode{
				Name: "ptah_test.f_c", Returns: "int", Volatility: "IMMUTABLE", Body: "RETURN 1",
			})

			c.Assert(err, qt.IsNil)
			c.Check(r.Output(), qt.Contains, "CREATE FUNCTION `ptah_test`.`f_c`() RETURNS int DETERMINISTIC RETURN 1;")
			c.Check(r.Output(), qt.Not(qt.Contains), "`ptah_test.f_c`")
		})
	}
}

// TestVisitCreateFunction_RendersExactlyOneStatement pins the invariant that
// makes an element of GetOrderedCreateStatements executable as it stands.
//
// The visitor used to emit its own `DROP FUNCTION IF EXISTS` in front of every
// CREATE, which put two statements in one element. The planner splits its
// output before executing, so that path worked; the compatibility dev-database
// path does not. `materializeOnDev` passes each element unchanged to
// ExecuteSQL, and convertMySQLURL does not enable go-sql-driver's
// multiStatements option, so materializing any desired schema containing a
// function failed. Measured through dbschema.ConnectToDatabase on both engines
// with the default DSN:
//
//	Error 1064 (42000): ... near 'CREATE FUNCTION `p_fn`(a INT) RETURNS int
//	DETERMINISTIC SQL SECURITY INVOKER ...' at line 2
//
// The drop a replacement still needs is now a separate node the MySQL-family
// planner emits; see its own test for that half. Counting semicolons is the
// cheapest statement of the rule that a mutant restoring the prefix fails.
func TestVisitCreateFunction_RendersExactlyOneStatement(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			r := newRenderer(dialect)

			err := r.VisitCreateFunction(&ast.CreateFunctionNode{
				Name: "fn", Parameters: "a INT", Returns: "int",
				Volatility: "IMMUTABLE", Security: "INVOKER", Body: "RETURN a + 1",
			})

			c.Assert(err, qt.IsNil)
			c.Check(strings.Count(r.Output(), ";"), qt.Equals, 1)
			c.Check(r.Output(), qt.Not(qt.Contains), "DROP FUNCTION")
		})
	}
}

// TestVisitCreateFunction_VolatilityIsDistinguishableAfterARead pins the write
// half of the volatility round trip at the renderer.
//
// STABLE and VOLATILE used to render the same characteristic, so a read could
// not tell them apart and a declared STABLE function reported
// `volatility: VOLATILE -> STABLE` after a successful apply and planned the
// same destructive replacement forever. The measurements behind the three
// clauses are in mysqlroutine.Characteristic; this holds the renderer to them.
func TestVisitCreateFunction_VolatilityIsDistinguishableAfterARead(t *testing.T) {
	tests := []struct {
		name       string
		volatility string
		want       string
	}{
		{name: "immutable", volatility: "IMMUTABLE", want: "DETERMINISTIC RETURN 1;"},
		{name: "stable", volatility: "STABLE", want: "NOT DETERMINISTIC NO SQL RETURN 1;"},
		{name: "volatile", volatility: "VOLATILE", want: "READS SQL DATA RETURN 1;"},
	}

	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				r := newRenderer(dialect)

				err := r.VisitCreateFunction(&ast.CreateFunctionNode{
					Name: "fn", Returns: "int", Volatility: test.volatility, Body: "RETURN 1",
				})

				c.Assert(err, qt.IsNil)
				c.Check(r.Output(), qt.Contains, test.want)
			})
		}
	}
}

// TestVisitCreateFunction_RefusesValuesItCannotRepresent holds the refusal seam.
//
// Both of these used to be silently dropped, and both produced the same
// permanent drift: an unknown security mode emitted no clause at all, so MySQL
// applied its DEFINER default and every later comparison reported
// `security: DEFINER -> INVKOER` -- measured live on both engines. An operator
// who asked for invoker rights got definer rights AND a diff that never closed.
//
// The refusal happens before anything is written, which is load-bearing: the
// planner emits a DROP in front of this node, and a CREATE refused after that
// drop was rendered would leave a migration whose only effect is deletion.
func TestVisitCreateFunction_RefusesValuesItCannotRepresent(t *testing.T) {
	tests := []struct {
		name string
		node *ast.CreateFunctionNode
		want string
	}{
		{
			name: "misspelled security mode",
			node: &ast.CreateFunctionNode{
				Name: "fn", Returns: "int", Volatility: "IMMUTABLE",
				Language: "sql", Security: "INVKOER", Body: "RETURN 1",
			},
			want: "INVKOER",
		},
		{
			name: "unknown volatility",
			node: &ast.CreateFunctionNode{
				Name: "fn", Returns: "int", Volatility: "STABEL",
				Language: "sql", Body: "RETURN 1",
			},
			want: "STABEL",
		},
		{
			// REAL reads back as double or float depending on the connection's
			// sql_mode, so no single normalization is right for both.
			name: "sql-mode-dependent REAL",
			node: &ast.CreateFunctionNode{
				Name: "fn", Returns: "REAL", Volatility: "IMMUTABLE",
				Language: "sql", Body: "RETURN 1",
			},
			want: "REAL_AS_FLOAT",
		},
		{
			// NATIONAL VARCHAR and VARCHAR report the same DTD_IDENTIFIER and
			// differ only in a charset column this comparison does not read.
			name: "national character set parameter",
			node: &ast.CreateFunctionNode{
				Name: "fn", Parameters: "a NATIONAL VARCHAR(10)", Returns: "int",
				Volatility: "IMMUTABLE", Language: "sql", Body: "RETURN 1",
			},
			want: "character set",
		},
	}

	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				r := newRenderer(dialect)

				err := r.VisitCreateFunction(test.node)

				c.Assert(err, qt.IsNotNil)
				c.Check(err.Error(), qt.Contains, test.want)
				c.Check(r.Output(), qt.Equals, "")
			})
		}
	}
}
