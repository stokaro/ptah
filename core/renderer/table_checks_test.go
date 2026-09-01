package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
)

// checksDatabase is one table declaring its check expressions the way the
// `checks` table attribute does: a list of expressions, none of them named.
func checksDatabase(checks ...string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "Product",
			Name:       "products",
			Checks:     checks,
		}},
		Fields: []schemamodel.Field{
			{StructName: "Product", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Product", Name: "price", Type: "INTEGER"},
		},
	}
}

// TestTableChecks_HappyPath renders a table-level check expression on every
// dialect.
//
// The attribute has been parsed into [schemamodel.Table.Checks] since the first
// commit and read by nothing on the way to SQL, so a table declaring
// `checks="price > 0"` reached the server with no CHECK on it and every later
// comparison agreed, because both sides were reading the same empty answer
// (stokaro/ptah#2590).
//
// The constraint is NAMED although the declaration names none. An unnamed CHECK
// reads back under a name the server invented, which the comparison cannot
// predict; see schemaprep.TableCheckConstraints and the convergence test in
// migration/schemadiff.
func TestTableChecks_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    string
	}{
		{name: "postgres", dialect: "postgres", want: `CONSTRAINT "products_check" CHECK (price > 0)`},
		{name: "mysql", dialect: "mysql", want: "CONSTRAINT `products_check` CHECK (price > 0)"},
		{name: "mariadb", dialect: "mariadb", want: "CONSTRAINT `products_check` CHECK (price > 0)"},
		{name: "sqlite", dialect: "sqlite", want: `CONSTRAINT "products_check" CHECK (price > 0)`},
		{name: "sqlserver", dialect: "sqlserver", want: `CONSTRAINT [products_check] CHECK (price > 0)`},
		{name: "oracle", dialect: "oracle", want: "CONSTRAINT products_check CHECK (price > 0)"},
		{name: "clickhouse", dialect: "clickhouse", want: "CONSTRAINT products_check CHECK (price > 0)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatements(checksDatabase("price > 0"), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, test.want)
		})
	}
}

// TestTableChecks_EveryDeclaredExpressionIsRendered is the control for the test
// above: rendering only the first entry of the list would satisfy it.
//
// It also pins the second entry's name apart from the first. One name for both
// is what ClickHouse's own synthesis produced -- it derives `<table>_check`
// from the table alone -- and two constraints cannot share a name.
func TestTableChecks_EveryDeclaredExpressionIsRendered(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(
		checksDatabase("price > 0", "id <> 0"), "postgres")

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, `CONSTRAINT "products_check" CHECK (price > 0)`)
	c.Assert(sql, qt.Contains, `CONSTRAINT "products_check1" CHECK (id <> 0)`)
}

// TestTableChecks_ADeclarationWithNoChecksRendersNone is the acceptance control
// for the two tests above. Without it a conversion that emitted a CHECK for
// every table would pass them both.
func TestTableChecks_ADeclarationWithNoChecksRendersNone(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatements(checksDatabase(), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "CHECK")
}

// TestTableChecks_AnAnnotatedTableRendersItsChecks drives the surface the
// attribute is documented on, so the test fails if the parser and the renderer
// stop agreeing about where the expressions live.
func TestTableChecks_AnAnnotatedTableRendersItsChecks(t *testing.T) {
	c := qt.New(t)

	const source = `package models

//ptah:schema:table name="products" checks="price > 0"
type Product struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//ptah:schema:field name="price" type="INTEGER"
	Price int64
}
`

	database, err := goschema.ParseSource("models.go", source)
	c.Assert(err, qt.IsNil)

	statements, renderErr := renderer.GetOrderedCreateStatements(&database, "postgres")

	c.Assert(renderErr, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, `CONSTRAINT "products_check" CHECK (price > 0)`)
}

// TestTableChecks_AnExplicitConstraintSupersedesTheSameExpression covers the
// collision between the two spellings of one constraint.
//
// A table may carry the expression in its `checks` list and again in an
// explicit CHECK constraint. Rendering both produces two CHECKs the author
// asked for once; the explicit one survives because it carries the name they
// chose, which is what a later DROP CONSTRAINT addresses.
func TestTableChecks_AnExplicitConstraintSupersedesTheSameExpression(t *testing.T) {
	c := qt.New(t)

	database := checksDatabase("price > 0")
	database.Constraints = []schemamodel.Constraint{{
		StructName:      "Product",
		Name:            "products_price_positive",
		Type:            "CHECK",
		Table:           "products",
		CheckExpression: "price > 0",
	}}

	statements, err := renderer.GetOrderedCreateStatements(database, "postgres")

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(strings.Count(sql, "CHECK (price > 0)"), qt.Equals, 1)
	c.Assert(sql, qt.Contains, `CONSTRAINT "products_price_positive" CHECK (price > 0)`)
	c.Assert(sql, qt.Not(qt.Contains), "products_check")
}

// TestTableChecks_ADifferentExpressionIsNotReplaced is the control for the test
// above: a replacement that matched on the constraint kind alone would delete a
// check the author wrote.
func TestTableChecks_ADifferentExpressionIsNotReplaced(t *testing.T) {
	c := qt.New(t)

	database := checksDatabase("price > 0")
	database.Constraints = []schemamodel.Constraint{{
		StructName:      "Product",
		Name:            "products_id_positive",
		Type:            "CHECK",
		Table:           "products",
		CheckExpression: "id > 0",
	}}

	statements, err := renderer.GetOrderedCreateStatements(database, "postgres")

	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, `CONSTRAINT "products_check" CHECK (price > 0)`)
	c.Assert(sql, qt.Contains, `CONSTRAINT "products_id_positive" CHECK (id > 0)`)
}
