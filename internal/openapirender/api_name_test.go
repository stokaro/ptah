package openapirender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	yaml "go.yaml.in/yaml/v3"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/openapirender"
)

// apiNameFixture is one table whose storage names are deliberately unlike the
// names a published API would use.
func apiNameFixture(fields ...goschema.Field) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "Invoice", Name: "invoices"}},
		Fields: fields,
	}
}

// A column exports under its API name where one is declared, and the property
// carrying it is the one the document names as required — a document that
// requires a property it does not define is not loadable, so the two have to
// move together (stokaro/ptah#905).
func TestRenderUsesTheDeclaredAPIName(t *testing.T) {
	c := qt.New(t)

	res, err := openapirender.Render(apiNameFixture(
		goschema.Field{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
		goschema.Field{
			StructName: "Invoice", Name: "billing_amount_minor",
			APIName: "amount", Type: "INTEGER",
		},
		goschema.Field{StructName: "Invoice", Name: "note", Type: "TEXT", Nullable: true},
	), openapirender.Options{})
	c.Assert(err, qt.IsNil)

	var doc map[string]any
	c.Assert(yaml.Unmarshal(res.Data, &doc), qt.IsNil)
	invoices := doc["components"].(map[string]any)["schemas"].(map[string]any)["invoices"].(map[string]any)
	props := invoices["properties"].(map[string]any)

	_, aliased := props["amount"]
	c.Assert(aliased, qt.IsTrue, qt.Commentf("properties=%v", props))
	_, storage := props["billing_amount_minor"]
	c.Assert(storage, qt.IsFalse,
		qt.Commentf("the column name must not also appear, or the field is published twice"))

	// The undeclared columns are untouched.
	_, plain := props["note"]
	c.Assert(plain, qt.IsTrue)

	var required []string
	for _, name := range invoices["required"].([]any) {
		required = append(required, name.(string))
	}
	c.Assert(required, qt.Contains, "amount")
	c.Assert(required, qt.Not(qt.Contains), "billing_amount_minor")
}

// The refusal happens before anything is written. An alias that shadows another
// column would drop that column from the document, and the document has no way
// to record that it lost one.
func TestRenderRefusesAnAPINameCollision(t *testing.T) {
	c := qt.New(t)

	res, err := openapirender.Render(apiNameFixture(
		goschema.Field{StructName: "Invoice", Name: "amount", Type: "INTEGER"},
		goschema.Field{
			StructName: "Invoice", Name: "billing_amount_minor",
			APIName: "amount", Type: "INTEGER",
		},
	), openapirender.Options{})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `exports two columns as "amount"`)
	c.Assert(err.Error(), qt.Contains, "billing_amount_minor")
	c.Assert(res.Data, qt.HasLen, 0, qt.Commentf("nothing may be written on the refusing path"))
}

// A table exports under its API name, and the diagnostic path follows it: the
// path is a coordinate inside the document, so it has to resolve there.
func TestRenderUsesTheDeclaredTableAPIName(t *testing.T) {
	c := qt.New(t)

	res, err := openapirender.Render(&goschema.Database{
		Tables: []goschema.Table{{StructName: "Invoice", Name: "billing_invoices", APIName: "invoices"}},
		Fields: []goschema.Field{
			{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
			{StructName: "Invoice", Name: "quirk", Type: "some_unknown_type", Nullable: true},
		},
	}, openapirender.Options{})
	c.Assert(err, qt.IsNil)

	var doc map[string]any
	c.Assert(yaml.Unmarshal(res.Data, &doc), qt.IsNil)
	schemas := doc["components"].(map[string]any)["schemas"].(map[string]any)

	_, aliased := schemas["invoices"]
	c.Assert(aliased, qt.IsTrue, qt.Commentf("schemas=%v", schemas))
	_, storage := schemas["billing_invoices"]
	c.Assert(storage, qt.IsFalse)

	c.Assert(res.Diagnostics, qt.HasLen, 1)
	c.Assert(res.Diagnostics[0].Path, qt.Equals, "components.schemas.invoices.properties.quirk")
}

// Two tables published under one name would drop one of them from the
// document, with nothing left in it to say so.
func TestRenderRefusesATableAPINameCollision(t *testing.T) {
	c := qt.New(t)

	res, err := openapirender.Render(&goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Invoice", Name: "invoices"},
			{StructName: "Billing", Name: "billing_invoices", APIName: "invoices"},
		},
		Fields: []goschema.Field{
			{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
			{StructName: "Billing", Name: "id", Type: "BIGSERIAL", Primary: true},
		},
	}, openapirender.Options{})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `two tables export as "invoices"`)
	c.Assert(err.Error(), qt.Contains, "billing_invoices")
	c.Assert(res.Data, qt.HasLen, 0)
}

// The documented limitation this exists for: DECIMAL maps to `number`, which
// can lose decimal precision. Carrying it as text keeps the digits.
func TestRenderUsesTheDeclaredAPIType(t *testing.T) {
	c := qt.New(t)

	res, err := openapirender.Render(apiNameFixture(
		goschema.Field{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
		goschema.Field{StructName: "Invoice", Name: "plain", Type: "DECIMAL(12,2)"},
		goschema.Field{StructName: "Invoice", Name: "exact", Type: "DECIMAL(12,2)", APIType: "TEXT"},
	), openapirender.Options{})
	c.Assert(err, qt.IsNil)

	var doc map[string]any
	c.Assert(yaml.Unmarshal(res.Data, &doc), qt.IsNil)
	props := doc["components"].(map[string]any)["schemas"].(map[string]any)["invoices"].(map[string]any)["properties"].(map[string]any)

	c.Assert(props["plain"].(map[string]any)["type"], qt.Equals, "number")
	c.Assert(props["exact"].(map[string]any)["type"], qt.Equals, "string")
}

// An override the mapping cannot honor is refused, where an unrecognized
// COLUMN type is only warned about. The difference is deliberate: one is a
// fact about the schema, the other is a declaration that would do nothing.
func TestRenderRefusesAnUnknownAPIType(t *testing.T) {
	c := qt.New(t)

	res, err := openapirender.Render(apiNameFixture(
		goschema.Field{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
		goschema.Field{StructName: "Invoice", Name: "amount", Type: "DECIMAL(12,2)", APIType: "money_ish"},
	), openapirender.Options{})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, `declares api_type "money_ish"`)
	c.Assert(err.Error(), qt.Contains, "OpenAPI projection does not recognize")
	c.Assert(err.Error(), qt.Contains, `keep the column's own type "DECIMAL(12,2)"`)
	c.Assert(res.Data, qt.HasLen, 0)
}

// The control that keeps the refusal from being a blanket rejection of unmapped
// types: a column whose OWN type is unrecognized still exports, with a warning.
func TestRenderStillWarnsForAnUnknownColumnType(t *testing.T) {
	c := qt.New(t)

	res, err := openapirender.Render(apiNameFixture(
		goschema.Field{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
		goschema.Field{StructName: "Invoice", Name: "quirk", Type: "money_ish", Nullable: true},
	), openapirender.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(res.Diagnostics, qt.HasLen, 1)
	c.Assert(res.Diagnostics[0].Message, qt.Contains, "money_ish")
}

// The override reaches enum resolution in both directions. The type mapping
// alone knows only scalars, so asking it by itself would refuse the second
// case -- which is the one worth having: on a dialect with no native enum the
// column IS text, and publishing it as the enum is what the contract wants.
func TestRenderProjectsEnumsBothWays(t *testing.T) {
	c := qt.New(t)

	db := apiNameFixture(
		goschema.Field{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
		goschema.Field{StructName: "Invoice", Name: "flattened", Type: "invoice_state", APIType: "TEXT"},
		goschema.Field{StructName: "Invoice", Name: "promoted", Type: "VARCHAR(32)", APIType: "invoice_state"},
	)
	db.Enums = []goschema.Enum{{Name: "invoice_state", Values: []string{"draft", "sent"}}}

	res, err := openapirender.Render(db, openapirender.Options{})
	c.Assert(err, qt.IsNil)

	var doc map[string]any
	c.Assert(yaml.Unmarshal(res.Data, &doc), qt.IsNil)
	props := doc["components"].(map[string]any)["schemas"].(map[string]any)["invoices"].(map[string]any)["properties"].(map[string]any)

	c.Assert(props["flattened"].(map[string]any)["enum"], qt.IsNil)
	c.Assert(props["promoted"].(map[string]any)["enum"], qt.DeepEquals, []any{"draft", "sent"})
}

// Inline enum values describe the stored column, and enum resolution consults
// them before the type. Left in place they answer first and the override does
// nothing -- silently, which this annotation exists to rule out.
func TestRenderOverridesInlineEnumValues(t *testing.T) {
	c := qt.New(t)

	res, err := openapirender.Render(apiNameFixture(
		goschema.Field{StructName: "Invoice", Name: "id", Type: "BIGSERIAL", Primary: true},
		goschema.Field{
			StructName: "Invoice", Name: "state", Type: "VARCHAR(16)",
			Enum: []string{"draft", "sent"}, APIType: "TEXT",
		},
	), openapirender.Options{})
	c.Assert(err, qt.IsNil)

	var doc map[string]any
	c.Assert(yaml.Unmarshal(res.Data, &doc), qt.IsNil)
	props := doc["components"].(map[string]any)["schemas"].(map[string]any)["invoices"].(map[string]any)["properties"].(map[string]any)

	c.Assert(props["state"].(map[string]any)["type"], qt.Equals, "string")
	c.Assert(props["state"].(map[string]any)["enum"], qt.IsNil)
}
