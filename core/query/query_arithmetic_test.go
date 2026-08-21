package query_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/query"
	"go.5x5.cz/ptah/core/renderer"
)

// TestArithmetic_ParenthesizesSoTheTreeSurvives is the reason this node renders
// its own parentheses.
//
// The two expressions below are DIFFERENT trees over the same three operands.
// Rendered without parentheses both become `a + b * c`, and the server then
// applies its own precedence — agreeing with one tree and silently rewriting
// the other. This is the assertion that reddens if anyone removes them
// (stokaro/ptah#941).
func TestArithmetic_ParenthesizesSoTheTreeSurvives(t *testing.T) {
	c := qt.New(t)
	sum := query.Mul(query.Add(query.ColExpr("a"), query.ColExpr("b")), query.ColExpr("c"))
	product := query.Add(query.ColExpr("a"), query.Mul(query.ColExpr("b"), query.ColExpr("c")))
	stmt := query.Select().From("t").ExprAs(sum, "l").ExprAs(product, "r").Build()

	sql, _, err := renderer.RenderSelect(stmt, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals,
		`SELECT (("a" + "b") * "c") AS "l", ("a" + ("b" * "c")) AS "r" FROM "t"`)
}

// TestArithmetic_RendersEveryOperator covers the five tokens.
func TestArithmetic_RendersEveryOperator(t *testing.T) {
	tests := []struct {
		name  string
		build func() ast.Expression
		want  string
	}{
		{name: "add", build: func() ast.Expression { return query.Add(query.ColExpr("a"), query.ColExpr("b")) }, want: `("a" + "b")`},
		{name: "sub", build: func() ast.Expression { return query.Sub(query.ColExpr("a"), query.ColExpr("b")) }, want: `("a" - "b")`},
		{name: "mul", build: func() ast.Expression { return query.Mul(query.ColExpr("a"), query.ColExpr("b")) }, want: `("a" * "b")`},
		{name: "div", build: func() ast.Expression { return query.Div(query.ColExpr("a"), query.ColExpr("b")) }, want: `("a" / "b")`},
		{name: "mod", build: func() ast.Expression { return query.Mod(query.ColExpr("a"), query.ColExpr("b")) }, want: `("a" % "b")`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			stmt := query.Select().From("t").ExprAs(test.build(), "v").Build()

			sql, _, err := renderer.RenderSelect(stmt, "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestArithmetic_BindsAValueOperand keeps a caller's number out of the
// statement text.
//
// An operand is an expression like any other, so a value travels as a bound
// parameter rather than being written into the SQL — the same property the
// comparison helpers have, asserted here because arithmetic is a new position
// for a value to appear in.
func TestArithmetic_BindsAValueOperand(t *testing.T) {
	c := qt.New(t)
	stmt := query.Select().From("t").
		ExprAs(query.Div(query.ColExpr("total"), query.Value(2)), "half").Build()

	sql, args, err := renderer.RenderSelect(stmt, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT ("total" / $1) AS "half" FROM "t"`)
	c.Assert(args, qt.DeepEquals, []any{2})
	c.Assert(sql, qt.Not(qt.Contains), "2)")
}

// TestFunc_CallsANonAggregateFunction covers the builder helper.
//
// The renderer already emitted these; what was missing was a way to ask for one
// without assembling the AST by hand.
func TestFunc_CallsANonAggregateFunction(t *testing.T) {
	c := qt.New(t)
	stmt := query.Select().From("users").
		ExprAs(query.Func("COALESCE", query.ColExpr("nick"), query.Value("anon")), "n").Build()

	sql, args, err := renderer.RenderSelect(stmt, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, `SELECT COALESCE("nick", $1) AS "n" FROM "users"`)
	// The name is a keyword, emitted verbatim; the argument is bound.
	c.Assert(args, qt.DeepEquals, []any{"anon"})
}

// TestFunc_RefusesANameThatIsNotAnIdentifier is the safety property.
//
// A function name is emitted VERBATIM and never quoted, so it is the one place
// in this builder where text reaches the statement unescaped. The renderer
// refuses anything that is not a simple identifier rather than emit it.
func TestFunc_RefusesANameThatIsNotAnIdentifier(t *testing.T) {
	tests := []string{
		`LOWER("x"); DROP TABLE users; --`,
		"LOWER(",
		"",
		"has space",
	}

	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			stmt := query.Select().From("t").ExprAs(query.Func(name, query.ColExpr("a")), "v").Build()

			sql, _, err := renderer.RenderSelect(stmt, "postgres")

			c.Assert(err, qt.IsNotNil)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestArithmetic_ComposesWithComparisons keeps the node usable where a
// predicate wants it, and pins the argument order across the two.
func TestArithmetic_ComposesWithComparisons(t *testing.T) {
	c := qt.New(t)
	stmt := query.Select("id").From("orders").
		Where(query.And(
			query.Eq("status", "open"),
			query.Gt("total", 10),
		)).
		ExprAs(query.Mul(query.ColExpr("total"), query.Value(100)), "cents").
		Build()

	sql, args, err := renderer.RenderSelect(stmt, "postgres")

	c.Assert(err, qt.IsNil)
	// The projection is emitted before WHERE, so its bound value is numbered
	// first.
	c.Assert(sql, qt.Equals,
		`SELECT "id", ("total" * $1) AS "cents" FROM "orders" `+
			`WHERE ("status" = $2 AND "total" > $3)`)
	c.Assert(args, qt.DeepEquals, []any{100, "open", 10})
}

// TestArithmetic_RefusesAnOperatorOutsideTheEnum covers the guard no builder
// helper can reach.
//
// Add through Mod are the only five spellings, so a caller using this package
// cannot produce an out-of-range operator. A caller assembling the AST directly
// can, and the renderer has to refuse rather than emit an expression with an
// empty operator between its operands — `("a"  "b")`, which parses as
// something else entirely on some engines and as nothing on others.
func TestArithmetic_RefusesAnOperatorOutsideTheEnum(t *testing.T) {
	c := qt.New(t)
	stmt := query.Select().From("t").ExprAs(&ast.Arithmetic{
		Left:     &ast.ColumnRef{Name: "a"},
		Operator: ast.ArithmeticOperator(99),
		Right:    &ast.ColumnRef{Name: "b"},
	}, "v").Build()

	sql, _, err := renderer.RenderSelect(stmt, "postgres")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "unknown arithmetic operator")
	c.Assert(sql, qt.Equals, "")
}

// TestArithmeticOperator_StringIsEmptyOutsideTheEnum pins the value the
// renderer's check reads, so the two cannot drift apart.
func TestArithmeticOperator_StringIsEmptyOutsideTheEnum(t *testing.T) {
	tests := []struct {
		name string
		op   ast.ArithmeticOperator
		want string
	}{
		{name: "add", op: ast.OpAdd, want: "+"},
		{name: "modulo", op: ast.OpModulo, want: "%"},
		{name: "past the end", op: ast.ArithmeticOperator(99), want: ""},
		{name: "negative", op: ast.ArithmeticOperator(-1), want: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.op.String(), qt.Equals, test.want)
		})
	}
}
