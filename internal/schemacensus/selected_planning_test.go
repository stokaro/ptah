package schemacensus_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/featureplan"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/schemacensus"
	"ptah.run/migration/schemadiff"
)

type censusFault struct {
	engine.SchemaRuntime
	fail func() error
}

type censusValidationFault struct{ censusFault }

func (r censusValidationFault) ValidateSchema(context.Context, schemavalidation.Request) (schemavalidation.Result, error) {
	return schemavalidation.Result{Complete: true}, r.fail()
}

type censusComparisonFault struct{ censusFault }

func (r censusComparisonFault) CompareFeatures(context.Context, schemaext.ComparisonRequest) (schemaext.ComparisonResult, error) {
	return schemaext.ComparisonResult{Complete: true}, r.fail()
}

type censusPlanningFault struct{ censusFault }

func (r censusPlanningFault) PlanFeatures(context.Context, featureplan.Request) (featureplan.Result, error) {
	return featureplan.Result{}, r.fail()
}

type censusRenderingFault struct{ censusFault }

func (r censusRenderingFault) Render(context.Context, renderer.Request) (renderer.Result, error) {
	return renderer.Result{Complete: true, Fragments: []string{"unusable partial output"}}, r.fail()
}

func TestPlanCensusDiscardsFailedAndCanceledMeasurements(t *testing.T) {
	stages := []struct {
		name string
		wrap func(censusFault) engine.SchemaRuntime
	}{
		{"validation", func(f censusFault) engine.SchemaRuntime { return censusValidationFault{f} }},
		{"comparison", func(f censusFault) engine.SchemaRuntime { return censusComparisonFault{f} }},
		{"planning", func(f censusFault) engine.SchemaRuntime { return censusPlanningFault{f} }},
		{"rendering", func(f censusFault) engine.SchemaRuntime { return censusRenderingFault{f} }},
	}
	for _, stage := range stages {
		for _, failure := range []error{
			errors.New("provider connection failed"),
			schemaext.ErrUnknownCodec,
			&schemaext.UnknownCodecError{},
			&schemavalidation.RefusalError{},
			&schemadiff.RefusalError{},
			errors.Join((schemavalidation.Result{Complete: true, Diagnostics: []schemavalidation.Diagnostic{{
				Code: schemavalidation.InvalidSchema, Kind: "schema", Message: "completed refusal",
			}}}).Err("custom"), errors.New("provider failed alongside a refusal")),
			&ptaherr.CapabilityError{Err: ptaherr.ErrUnsupportedFeature, Message: "service failed before reporting diagnostics"},
			context.Canceled,
		} {
			t.Run(stage.name+"/"+failure.Error(), func(t *testing.T) {
				c := qt.New(t)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				calls := 0
				fault := censusFault{SchemaRuntime: must.Must(builtin.New()), fail: func() error {
					calls++
					return censusFailure(cancel, failure)
				}}
				observations, err := schemacensus.MeasurePlan(ctx, stage.wrap(fault))
				c.Assert(err, qt.ErrorIs, failure)
				c.Assert(observations, qt.IsNil)
				c.Assert(calls, qt.Equals, 1)
			})
		}
	}
}

type censusLateValidationFault struct {
	engine.SchemaRuntime
	calls   int
	failure error
}

func (r *censusLateValidationFault) ValidateSchema(ctx context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
	r.calls++
	if r.calls == 5 {
		return schemavalidation.Result{}, r.failure
	}
	return r.SchemaRuntime.ValidateSchema(ctx, request)
}

func TestPlanCensusDiscardsEarlierCellsAfterLateFailure(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("provider failed after earlier cells completed")
	runtime := &censusLateValidationFault{SchemaRuntime: must.Must(builtin.New()), failure: failure}
	observations, err := schemacensus.MeasurePlan(t.Context(), runtime)
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(observations, qt.IsNil)
	c.Assert(runtime.calls, qt.Equals, 5)
}

func censusFailure(cancel context.CancelFunc, failure error) error {
	if errors.Is(failure, context.Canceled) {
		cancel()
		return nil
	}
	return failure
}
