package engine_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaprojection"
	"ptah.run/engine"
)

type constraintProjectionFunc func(context.Context, schemaprojection.ConstraintRequest) (schemaprojection.ConstraintResult, error)

func (f constraintProjectionFunc) ProjectConstraints(ctx context.Context, request schemaprojection.ConstraintRequest) (schemaprojection.ConstraintResult, error) {
	return f(ctx, request)
}

func projectionRequest() schemaprojection.ConstraintRequest {
	before := schemaprojection.TableState{Table: catalog.Table{Name: "items", Columns: []catalog.Column{{Name: "id", ColumnDefault: new("0")}}}}
	after := before.Clone()
	after.Constraints = []catalog.Constraint{{Name: "key", TableName: "items", Type: "UNIQUE", ColumnNames: []string{"id"}}}
	return schemaprojection.ConstraintRequest{Target: "custom", Identifiers: identifier.ForDialect("postgres"), Capabilities: capability.Postgres18(),
		Before: before, After: after, Changes: []schemaprojection.ConstraintChange{{After: new(after.Constraints[0].Clone())}}}
}

func projectionProvider(service schemaprojection.ConstraintService) engine.Provider {
	return engine.Provider{ID: "example.org/projector", Targets: []engine.Target{{Name: "custom", Aliases: []string{"alternate"}, Constraints: service}}}
}

func TestConstraintProjectionSelectedServiceAndSnapshotOwnership(t *testing.T) {
	c := qt.New(t)
	var received schemaprojection.ConstraintRequest
	service := constraintProjectionFunc(func(_ context.Context, request schemaprojection.ConstraintRequest) (schemaprojection.ConstraintResult, error) {
		received = request
		*request.After.Table.Columns[0].ColumnDefault = "1"
		return schemaprojection.ConstraintResult{State: &request.After}, nil
	})
	provider := projectionProvider(service)
	runtime := mustRuntime(c, provider)
	provider.Targets[0].Constraints = nil
	request := projectionRequest()
	request.Target = " ALTERNATE "
	result, err := runtime.ProjectConstraints(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(received.Target, qt.Equals, "custom")
	c.Assert(*request.After.Table.Columns[0].ColumnDefault, qt.Equals, "0")
	c.Assert(*result.State.Table.Columns[0].ColumnDefault, qt.Equals, "1")
	*received.After.Table.Columns[0].ColumnDefault = "2"
	c.Assert(*result.State.Table.Columns[0].ColumnDefault, qt.Equals, "1")
	received.Changes[0].After.ColumnNames[0] = "mutated"
	c.Assert(request.Changes[0].After.ColumnNames, qt.DeepEquals, []string{"id"})
}

func TestConstraintProjectionRejectsInvalidResults(t *testing.T) {
	transport := errors.New("provider exited")
	cases := []struct {
		name    string
		service constraintProjectionFunc
		want    error
	}{
		{name: "missing outcome", want: schemaprojection.ErrInvalid, service: func(context.Context, schemaprojection.ConstraintRequest) (schemaprojection.ConstraintResult, error) {
			return schemaprojection.ConstraintResult{}, nil
		}},
		{name: "transport", want: transport, service: func(_ context.Context, r schemaprojection.ConstraintRequest) (schemaprojection.ConstraintResult, error) {
			return schemaprojection.ConstraintResult{State: &r.After}, transport
		}},
		{name: "state and unavailable", want: schemaprojection.ErrInvalid, service: func(_ context.Context, r schemaprojection.ConstraintRequest) (schemaprojection.ConstraintResult, error) {
			return schemaprojection.ConstraintResult{State: &r.After, Unavailable: "unknown"}, nil
		}},
		{name: "wrong table", want: schemaprojection.ErrInvalid, service: func(_ context.Context, r schemaprojection.ConstraintRequest) (schemaprojection.ConstraintResult, error) {
			r.After.Table.Name = "other"
			return schemaprojection.ConstraintResult{State: &r.After}, nil
		}},
		{name: "wrong index owner", want: schemaprojection.ErrInvalid, service: func(_ context.Context, r schemaprojection.ConstraintRequest) (schemaprojection.ConstraintResult, error) {
			r.After.Indexes = []catalog.Index{{Name: "key", TableName: "other"}}
			return schemaprojection.ConstraintResult{State: &r.After}, nil
		}},
		{name: "duplicate column", want: schemaprojection.ErrInvalid, service: func(_ context.Context, r schemaprojection.ConstraintRequest) (schemaprojection.ConstraintResult, error) {
			r.After.Table.Columns = append(r.After.Table.Columns, r.After.Table.Columns[0])
			return schemaprojection.ConstraintResult{State: &r.After}, nil
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := mustRuntime(c, projectionProvider(test.service))
			result, err := runtime.ProjectConstraints(t.Context(), projectionRequest())
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.DeepEquals, schemaprojection.ConstraintResult{})
		})
	}
}

func TestConstraintProjectionUnavailabilityAndCancellation(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, projectionProvider(nil))
	result, err := runtime.ProjectConstraints(t.Context(), projectionRequest())
	c.Assert(err, qt.IsNil)
	c.Assert(result.State, qt.IsNil)
	c.Assert(result.Unavailable, qt.Not(qt.Equals), "")
	request := projectionRequest()
	request.Target = "missing"
	result, err = runtime.ProjectConstraints(t.Context(), request)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(result.State, qt.IsNil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err = runtime.ProjectConstraints(ctx, projectionRequest())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result.State, qt.IsNil)
	var absent constraintProjectionFunc
	runtime, err = engine.New(projectionProvider(absent))
	c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
	c.Assert(runtime, qt.IsNil)
}

func TestConstraintProjectionRejectsTransitionOutsideCapturedState(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, projectionProvider(nil))
	request := projectionRequest()
	request.Changes[0].After.ColumnNames[0] = "other"
	result, err := runtime.ProjectConstraints(t.Context(), request)
	c.Assert(err, qt.ErrorIs, schemaprojection.ErrInvalid)
	c.Assert(result.State, qt.IsNil)
}

func TestConstraintProjectionDiscardsResultAfterCancellation(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	service := constraintProjectionFunc(func(_ context.Context, r schemaprojection.ConstraintRequest) (schemaprojection.ConstraintResult, error) {
		cancel()
		return schemaprojection.ConstraintResult{State: &r.After}, nil
	})
	runtime := mustRuntime(c, projectionProvider(service))
	result, err := runtime.ProjectConstraints(ctx, projectionRequest())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, schemaprojection.ConstraintResult{})
}
