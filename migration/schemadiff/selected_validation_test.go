package schemadiff_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

type selectedValidator struct {
	schemapreparation.Runtime
	validate func(context.Context, schemavalidation.Request) (schemavalidation.Result, error)
}

func (s selectedValidator) ValidateSchema(ctx context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
	return s.validate(ctx, request)
}

func TestCompareWithDatabaseInfoUsesSelectedValidation(t *testing.T) {
	c := qt.New(t)
	calls := 0
	caps := capability.Capabilities{capability.CreateIndexConcurrently: true}
	selected := selectedValidator{Runtime: must.Must(builtin.New()), validate: func(ctx context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
		calls++
		c.Assert(ctx, qt.Equals, t.Context())
		c.Assert(request.Target, qt.Equals, "postgres")
		c.Assert(request.Capabilities, qt.DeepEquals, caps)
		c.Assert(request.Schema.Tables, qt.HasLen, 2)
		return schemavalidation.Result{Complete: true}, nil
	}}
	desired := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "first", StructName: "First"}, {Name: "second", StructName: "Second"}}}
	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, &catalog.Database{}, catalog.ServerInfo{Dialect: "postgres", Capabilities: caps}, nil, selected)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesAdded, qt.HasLen, 2)
	c.Assert(calls, qt.Equals, 1)
	// Pure comparison needs only its declared comparison service.
	_, err = schemadiff.Compare(t.Context(), desired, &catalog.Database{}, selected.Runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
}

func TestCompareWithDatabaseInfoRefusesFailedValidationBeforeDiff(t *testing.T) {
	failure := errors.New("validator disconnected")
	for _, test := range []struct {
		name    string
		result  schemavalidation.Result
		failure error
		want    error
	}{
		{name: "schema", result: schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{Code: schemavalidation.UnsupportedFeature, Kind: "schema", Message: "provider refusal"}}}, want: ptaherr.ErrUnsupportedFeature},
		{name: "transport", result: schemavalidation.Result{Complete: true}, failure: failure, want: failure},
		{name: "incomplete", want: schemavalidation.ErrInvalidResult},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			selected := selectedValidator{Runtime: must.Must(builtin.New()), validate: func(context.Context, schemavalidation.Request) (schemavalidation.Result, error) {
				return test.result, test.failure
			}}
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), &schemamodel.Database{}, &catalog.Database{}, catalog.ServerInfo{Dialect: "postgres"}, nil, selected)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(diff, qt.IsNil)
		})
	}
}
