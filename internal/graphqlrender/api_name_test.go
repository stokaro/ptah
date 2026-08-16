package graphqlrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/graphqlrender"
)

func apiNameFixture(fields ...goschema.Field) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "Invoice", Name: "invoices"}},
		Fields: fields,
	}
}

// The declared API name is what the SDL publishes, and the column name is not
// published alongside it (stokaro/ptah#905).
func TestRenderPublishesTheDeclaredAPIName(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(apiNameFixture(
		goschema.Field{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
		goschema.Field{
			StructName: "Invoice", Name: "billing_amount_minor",
			APIName: "amount", Type: "INTEGER",
		},
	), graphqlrender.Options{})
	c.Assert(err, qt.IsNil)

	sdl := string(res.Data)
	c.Assert(sdl, qt.Contains, "amount: Int!")
	c.Assert(sdl, qt.Not(qt.Contains), "billing_amount_minor")
}

// Sanitization runs on the API name exactly as it runs on a column name: an
// alias is an arbitrary annotation string too, and a GraphQL field name that is
// not a legal identifier fails to build.
//
// The diagnostic keeps naming the COLUMN. A warning about a name the reader
// cannot find in their schema source is a warning they cannot act on.
func TestRenderSanitizesAnIllegalAPIName(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(apiNameFixture(
		goschema.Field{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
		goschema.Field{
			StructName: "Invoice", Name: "billing_amount_minor",
			APIName: "amount-minor", Type: "INTEGER",
		},
	), graphqlrender.Options{})
	c.Assert(err, qt.IsNil)

	sdl := string(res.Data)
	c.Assert(sdl, qt.Contains, "amount_minor")
	c.Assert(sdl, qt.Not(qt.Contains), "amount-minor")
	c.Assert(diagnosticPaths(res), qt.Contains, "type Invoice.billing_amount_minor")
}

// diagnosticPaths collects the paths a render reported, so an assertion can name
// the one it expects instead of walking the slice in the test body.
func diagnosticPaths(res graphqlrender.Result) []string {
	paths := make([]string, 0, len(res.Diagnostics))
	for _, d := range res.Diagnostics {
		paths = append(paths, d.Path)
	}
	return paths
}

// A declared collision is refused rather than warned about. The existing
// warn-and-omit path is for names that only collide AFTER GraphQL sanitization,
// which is a naming-rules artifact; two columns explicitly published under one
// name is an authoring mistake, and dropping one of them silently is what the
// refusal exists to prevent.
func TestRenderRefusesAnAPINameCollision(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(apiNameFixture(
		goschema.Field{StructName: "Invoice", Name: "amount", Type: "INTEGER"},
		goschema.Field{
			StructName: "Invoice", Name: "billing_amount_minor",
			APIName: "amount", Type: "INTEGER",
		},
	), graphqlrender.Options{})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `exports two columns as "amount"`)
	c.Assert(res.Data, qt.HasLen, 0, qt.Commentf("nothing may be written on the refusing path"))
}

// The GraphQL type name is derived from the table's API name, so a published
// `Invoice` survives the table underneath being renamed.
func TestRenderDerivesTheTypeNameFromTheTableAPIName(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(&goschema.Database{
		Tables: []goschema.Table{{StructName: "Invoice", Name: "billing_invoices", APIName: "invoices"}},
		Fields: []goschema.Field{
			{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
		},
	}, graphqlrender.Options{})
	c.Assert(err, qt.IsNil)

	sdl := string(res.Data)
	// Singularized and PascalCased from the API name, exactly as it would be
	// from a table name.
	c.Assert(sdl, qt.Contains, "type Invoice {")
	c.Assert(sdl, qt.Not(qt.Contains), "BillingInvoice")
}

func TestRenderRefusesATableAPINameCollision(t *testing.T) {
	c := qt.New(t)

	res, err := graphqlrender.Render(&goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Invoice", Name: "invoices"},
			{StructName: "Billing", Name: "billing_invoices", APIName: "invoices"},
		},
		Fields: []goschema.Field{
			{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
			{StructName: "Billing", Name: "id", Type: "BIGSERIAL", Primary: true},
		},
	}, graphqlrender.Options{})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `two tables export as "invoices"`)
	c.Assert(res.Data, qt.HasLen, 0)
}
