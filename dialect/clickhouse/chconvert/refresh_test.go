package chconvert_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chconvert"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

func refreshConversionRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: "example.org/clickhouse", Targets: []engine.Target{{Name: "clickhouse"}},
		Codecs:      chschema.RefreshCodecs(),
		Conversions: []engine.Conversion{{Target: "clickhouse", Kinds: []schemaext.Kind{chschema.RefreshKind}, Service: chconvert.Service{}}},
	}))
}

// A captured schedule becomes the declaration that recreates it, and that
// declaration predicts the same observation, clause for clause.
func TestRefreshConversionPreservesEveryClause(t *testing.T) {
	c := qt.New(t)
	observed := &chschema.ObservedRefresh{Schedule: chschema.Schedule{
		Mode: chschema.RefreshEvery, Interval: "1 DAY", Offset: "2 HOUR", Randomize: "30 MINUTE",
		DependsOn: []string{"analytics.source"}, Append: true,
	}}
	runtime := refreshConversionRuntime()

	declared, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{
		Target: "clickhouse", From: schemaext.Observed, To: schemaext.Desired, Values: []schemaext.Value{observed},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(declared, qt.DeepEquals, []schemaext.Value{&chschema.DesiredRefresh{Schedule: observed.Schedule}})
	predicted, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{
		Target: "clickhouse", From: schemaext.Desired, To: schemaext.Observed, Values: declared,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(predicted, qt.DeepEquals, []schemaext.Value{observed})
	declared[0].(*chschema.DesiredRefresh).DependsOn[0] = "mutated"
	c.Assert(observed.DependsOn, qt.DeepEquals, []string{"analytics.source"})
}

func TestRefreshConversion_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name     string
		from, to schemaext.Representation
		value    schemaext.Value
	}{
		{"an invalid declaration", schemaext.Desired, schemaext.Observed, &chschema.DesiredRefresh{Schedule: chschema.Schedule{Mode: chschema.RefreshAfter, Interval: "1 HOUR", Offset: "1 MINUTE"}}},
		{"an invalid observation", schemaext.Observed, schemaext.Desired, &chschema.ObservedRefresh{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			values, err := (chconvert.Service{}).ConvertFeatures(t.Context(), schemaext.ConversionRequest{
				Target: "clickhouse", From: test.from, To: test.to, Values: []schemaext.Value{test.value},
			})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(values, qt.IsNil)
		})
	}
}
