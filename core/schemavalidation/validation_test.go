package schemavalidation_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
)

type validationFunc func(context.Context, schemavalidation.Request) (schemavalidation.Result, error)

func (f validationFunc) ValidateSchema(ctx context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
	return f(ctx, request)
}

func validRequest() schemavalidation.Request {
	return schemavalidation.Request{Target: "custom", Schema: &schemamodel.Database{}}
}

func TestValidatePreservesTheBatchAndSnapshotsFactAndReplyContainers(t *testing.T) {
	c := qt.New(t)
	request := validRequest()
	request.Schema.Tables = []schemamodel.Table{{Name: "first"}, {Name: "second"}}
	request.Capabilities = capability.Capabilities{capability.TransactionalDDL: true}
	request.NoSkipped = true
	request.Identifiers.ResolvedNames = []identifier.ResolvedName{{Name: "Orders", Key: "orders"}}
	diagnostics := []schemavalidation.Diagnostic{{Code: schemavalidation.InvalidSchema, Kind: "table", Object: "second", Message: "rejected declaration"}}
	calls := 0
	service := validationFunc(func(ctx context.Context, received schemavalidation.Request) (schemavalidation.Result, error) {
		calls++
		c.Assert(ctx, qt.Equals, t.Context())
		c.Assert(received, qt.DeepEquals, request)
		received.Schema.Tables = nil
		received.Capabilities[capability.TransactionalDDL] = false
		received.Identifiers.ResolvedNames[0].Key = "changed"
		return schemavalidation.Result{Complete: true, Diagnostics: diagnostics}, nil
	})
	result, err := schemavalidation.Validate(t.Context(), service, request)
	c.Assert(err, qt.IsNil)
	diagnostics[0].Message = "changed after return"
	c.Assert(result.Diagnostics[0].Message, qt.Equals, "rejected declaration")
	c.Assert(request.Capabilities[capability.TransactionalDDL], qt.IsTrue)
	c.Assert(request.Schema.Tables, qt.HasLen, 2)
	c.Assert(request.Identifiers.ResolvedNames[0].Key, qt.Equals, "orders")
	c.Assert(calls, qt.Equals, 1)
}

func TestValidateRefusesIncompleteAndMalformedReplies(t *testing.T) {
	for _, reply := range []schemavalidation.Result{
		{},
		{Diagnostics: []schemavalidation.Diagnostic{{Code: schemavalidation.InvalidSchema, Kind: "schema", Message: "partial"}}},
		{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{Code: "future", Kind: "schema", Message: "unknown"}}},
		{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{Code: schemavalidation.InvalidSchema, Message: "missing kind"}}},
		{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{Code: schemavalidation.InvalidSchema, Kind: "schema", Message: " "}}},
	} {
		c := qt.New(t)
		service := validationFunc(func(context.Context, schemavalidation.Request) (schemavalidation.Result, error) { return reply, nil })
		result, err := schemavalidation.Validate(t.Context(), service, validRequest())
		c.Assert(err, qt.ErrorIs, schemavalidation.ErrInvalidResult)
		c.Assert(result, qt.DeepEquals, schemavalidation.Result{})
	}
}

func TestValidateDiscardsFailedAndCanceledDiagnostics(t *testing.T) {
	failure := errors.New("provider connection failed")
	for _, test := range []struct {
		name    string
		failure error
		want    error
	}{
		{name: "service failure", failure: failure, want: failure},
		{name: "cancellation", want: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			service := validationFunc(func(context.Context, schemavalidation.Request) (schemavalidation.Result, error) {
				cancel()
				return schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{Code: schemavalidation.InvalidSchema, Kind: "schema", Message: "unusable"}}}, test.failure
			})
			result, err := schemavalidation.Validate(ctx, service, validRequest())
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.DeepEquals, schemavalidation.Result{})
		})
	}
}

func TestValidateRequiresContextServiceAndDeclaration(t *testing.T) {
	calls := 0
	service := validationFunc(func(context.Context, schemavalidation.Request) (schemavalidation.Result, error) {
		calls++
		return schemavalidation.Result{Complete: true}, nil
	})
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, test := range []struct {
		name    string
		ctx     context.Context
		service schemavalidation.Service
		schema  *schemamodel.Database
		want    error
	}{
		{name: "no context", service: service, schema: &schemamodel.Database{}, want: schemaext.ErrInvalidValue},
		{name: "no service", ctx: t.Context(), schema: &schemamodel.Database{}, want: schemaext.ErrInvalidValue},
		{name: "typed nil service", ctx: t.Context(), service: validationFunc(nil), schema: &schemamodel.Database{}, want: schemaext.ErrInvalidValue},
		{name: "no schema", ctx: t.Context(), service: service, want: ptaherr.ErrInvalidSchemaDiff},
		{name: "canceled", ctx: canceled, service: service, schema: &schemamodel.Database{}, want: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := schemavalidation.Validate(test.ctx, test.service, schemavalidation.Request{Target: "custom", Schema: test.schema})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.DeepEquals, schemavalidation.Result{})
		})
	}
	qt.New(t).Assert(calls, qt.Equals, 0)
}

func TestValidationDiagnosticsRetainTypedRefusals(t *testing.T) {
	c := qt.New(t)
	result := schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{
		{Code: schemavalidation.UnsupportedFeature, Kind: "schema", Feature: "custom indexes", Message: "index mode is unavailable"},
		{Code: schemavalidation.InvalidSchema, Kind: "table", Object: "items", Message: "table is invalid"},
	}}
	err := result.Err("custom")
	var refusal *schemavalidation.RefusalError
	c.Assert(err, qt.ErrorAs, &refusal)
	c.Assert(refusal.Target(), qt.Equals, "custom")
	c.Assert(refusal.Diagnostics(), qt.DeepEquals, result.Diagnostics)
	result.Diagnostics[0].Message = "mutated result"
	copyOfDiagnostics := refusal.Diagnostics()
	copyOfDiagnostics[0].Message = "mutated accessor"
	c.Assert(refusal.Diagnostics()[0].Message, qt.Equals, "index mode is unavailable")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	var unsupported *ptaherr.CapabilityError
	c.Assert(err, qt.ErrorAs, &unsupported)
	c.Assert(unsupported.Feature, qt.Equals, "custom indexes")
	c.Assert(unsupported.Dialect, qt.Equals, "custom")
	c.Assert((schemavalidation.Result{Complete: true}).Err("custom"), qt.IsNil)
	c.Assert((schemavalidation.Result{}).Err("custom"), qt.ErrorIs, schemavalidation.ErrInvalidResult)
	c.Assert((schemavalidation.Result{}).Err("custom"), qt.Not(qt.ErrorAs), &refusal)
}
