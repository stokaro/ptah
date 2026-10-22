//go:build integration

package providercli_test

import (
	"context"
	qt "github.com/frankban/quicktest"
	"ptah.run/core/coverage"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"testing"
)

func validationRequest() schemavalidation.Request {
	return schemavalidation.Request{Target: "fixture", Identifiers: comparisonRules(), Schema: &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "records", StructName: "Record"}}, Fields: []schemamodel.Field{{Name: "id", StructName: "Record", Type: "INTEGER", Comment: "retained"}},
		Schemas: []schemamodel.Schema{{Name: "main", Comment: "namespace comment"}}, NotDescribed: coverage.Set{}.WithKind(coverage.Role), Dependencies: map[string][]string{"records": {"parent"}}}}
}
func TestProviderValidatesWholeSchemaIndependently(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "schema-validation")
	request := validationRequest()
	result, err := runtime.ValidateSchema(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	request.NoSkipped = true
	result, err = runtime.ValidateSchema(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Code, qt.Equals, schemavalidation.OmittedDeclaration)
	c.Assert(request.Schema.Fields[0].Comment, qt.Equals, "retained")
	request.Schema = &schemamodel.Database{}
	result, err = runtime.ValidateSchema(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
}
func TestProviderValidationPreservesRefusalAndDiscardsMalformedSuffix(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "schema-validation")
	request := validationRequest()
	request.Schema.Tables = append(request.Schema.Tables, schemamodel.Table{Name: "invalid", StructName: "Invalid"})
	result, err := runtime.ValidateSchema(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Err("fixture"), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	request.Schema.Tables = append(request.Schema.Tables, schemamodel.Table{Name: "malformed", StructName: "Malformed"})
	result, err = runtime.ValidateSchema(c.Context(), request)
	c.Assert(err, qt.IsNotNil)
	c.Assert(result.Complete, qt.IsFalse)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	result, err = runtime.ValidateSchema(ctx, validationRequest())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
}
