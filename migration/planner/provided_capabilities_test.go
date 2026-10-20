package planner_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/planext"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// providingChange creates an object that provides hypertables once it exists,
// as an extension installing TimescaleDB does, when Creates is set.
type providingChange struct{ Creates bool }

func (*providingChange) Kind() schemaext.Kind { return "example.org/provider/change" }
func (v *providingChange) CloneChange() schemaext.ChangeValue {
	return &providingChange{Creates: v.Creates}
}
func (v *providingChange) ProvidedCapabilities(subject objectidentity.ID, caps capability.Capabilities) capability.Capabilities {
	return caps.With(capability.Hypertables, v.Creates && subject.Name.Source == "timescaledb")
}

// renderingRuntime is the bundled runtime with an owner that accounts for
// every change without an operation, recording the request it renders.
type renderingRuntime struct {
	*engine.Runtime
	rendered *renderer.Request
}

func (renderingRuntime) PlanFeatures(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
	result := featureplan.Result{Complete: true}
	for _, change := range request.Changes {
		result.Changes = append(result.Changes, featureplan.ChangePlan{Subject: change.Subject, Kind: change.Value.Kind(), Strategy: "test"})
	}
	return result, nil
}

func (r renderingRuntime) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	*r.rendered = request
	return r.Runtime.Render(ctx, request)
}

// TestRenderedPlan_WidensTheCapabilitiesAChangeProvides pins that a plan
// renders with what an object it creates provides, read from a connection
// opened before the object existed: the change answers for its own direction.
func TestRenderedPlan_WidensTheCapabilitiesAChangeProvides(t *testing.T) {
	tests := []struct {
		name    string
		creates bool
		want    bool
	}{
		{name: "a change that creates the provider", creates: true, want: true},
		{name: "control: one that does not", creates: false, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			subject := objectidentity.ID{Kind: "example.org/provider/object", Name: objectidentity.Part{Source: "timescaledb", Normalized: "timescaledb"}}
			diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{Subject: subject, Value: &providingChange{Creates: test.creates}}}}
			var rendered renderer.Request

			_, err := planner.GenerateSchemaDiffRenderedPlan(t.Context(), renderingRuntime{Runtime: must.Must(builtin.New()), rendered: &rendered},
				diff, "postgres", planext.Options{Capabilities: capability.Postgres17().With(capability.Hypertables, false)})

			c.Assert(err, qt.IsNil)
			c.Assert(rendered.Capabilities.Has(capability.Hypertables), qt.Equals, test.want)
		})
	}
}
