package engine_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/engine"
)

type loweringFunc func(context.Context, schemapreparation.LoweringRequest) (*schemamodel.Database, error)

func (f loweringFunc) LowerDesired(ctx context.Context, request schemapreparation.LoweringRequest) (*schemamodel.Database, error) {
	return f(ctx, request)
}

func loweringProvider(service schemapreparation.Lowering) engine.Provider {
	return engine.Provider{ID: "example.org/lowering", Targets: []engine.Target{{Name: "custom", Aliases: []string{"alternate"}, Lowering: service}}}
}

func loweringRequest() schemapreparation.LoweringRequest {
	return schemapreparation.LoweringRequest{
		Target:    "alternate",
		Desired:   &schemamodel.Database{Tables: []schemamodel.Table{{StructName: "Event", Name: "events"}}},
		Current:   &catalog.Database{Tables: []catalog.Table{{Name: "events"}}},
		Semantics: identifier.ForDialect("postgres"),
	}
}

func TestLowerDesired_HappyPath(t *testing.T) {
	t.Run("the selected target's service", func(t *testing.T) {
		c := qt.New(t)
		var received schemapreparation.LoweringRequest
		lowered := &schemamodel.Database{Tables: []schemamodel.Table{{StructName: "Event", Name: "lowered_events"}}}
		runtime := mustRuntime(c, loweringProvider(loweringFunc(func(_ context.Context, request schemapreparation.LoweringRequest) (*schemamodel.Database, error) {
			received = request
			return lowered, nil
		})))
		request := loweringRequest()

		got, err := runtime.LowerDesired(t.Context(), request)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.Equals, lowered)
		c.Assert(received.Target, qt.Equals, "custom")
		c.Assert(received.Desired, qt.Equals, request.Desired)
		c.Assert(received.Current, qt.Equals, request.Current)
		c.Assert(received.Semantics.Equal(request.Semantics), qt.IsTrue)
		c.Assert(request.Target, qt.Equals, "alternate")
	})

	t.Run("a target without one keeps the declaration", func(t *testing.T) {
		c := qt.New(t)
		runtime := mustRuntime(c, loweringProvider(nil))
		request := loweringRequest()

		got, err := runtime.LowerDesired(t.Context(), request)

		c.Assert(err, qt.IsNil)
		c.Assert(got, qt.Equals, request.Desired)
	})
}

var errLoweringFailed = errors.New("lowering failed")

func TestLowerDesired_FailurePath(t *testing.T) {
	lowerTo := func(lowered *schemamodel.Database, err error) schemapreparation.Lowering {
		return loweringFunc(func(context.Context, schemapreparation.LoweringRequest) (*schemamodel.Database, error) {
			return lowered, err
		})
	}
	cases := []struct {
		name    string
		service schemapreparation.Lowering
		target  string
		wantErr error
	}{
		{name: "unregistered target", service: lowerTo(&schemamodel.Database{}, nil), target: "unregistered", wantErr: ptaherr.ErrUnsupportedDialect},
		{name: "service error", service: lowerTo(nil, errLoweringFailed), target: "custom", wantErr: errLoweringFailed},
		{name: "nil result", service: lowerTo(nil, nil), target: "custom", wantErr: schemapreparation.ErrInvalid},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := mustRuntime(c, loweringProvider(tt.service))
			request := loweringRequest()
			request.Target = tt.target

			got, err := runtime.LowerDesired(t.Context(), request)

			c.Assert(err, qt.ErrorIs, tt.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

func TestLowerDesired_CanceledContext(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	runtime := mustRuntime(c, loweringProvider(loweringFunc(func(context.Context, schemapreparation.LoweringRequest) (*schemamodel.Database, error) {
		cancel()
		return &schemamodel.Database{}, nil
	})))
	request := loweringRequest()
	request.Target = "custom"

	got, err := runtime.LowerDesired(ctx, request)

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(got, qt.IsNil)
}

func TestNew_RefusesTypedNilLowering(t *testing.T) {
	c := qt.New(t)
	var service loweringFunc

	runtime, err := engine.New(loweringProvider(service))

	c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
	c.Assert(err, qt.ErrorMatches, `.*typed-nil lowering service`)
	c.Assert(runtime, qt.IsNil)
}
