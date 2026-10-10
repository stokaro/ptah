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

func rowPolicyConversionRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: "example.org/clickhouse", Targets: []engine.Target{{Name: "clickhouse"}},
		Codecs:      chschema.RowPolicyCodecs(),
		Conversions: []engine.Conversion{{Target: "clickhouse", Kinds: []schemaext.Kind{chschema.RowPolicyKind}, Service: chconvert.Service{}}},
	}))
}

// A captured policy becomes the declaration that recreates it, every default
// stated, and that declaration predicts the same observation.
func TestRowPolicyConversionPreservesThePolicy(t *testing.T) {
	c := qt.New(t)
	observed := &chschema.ObservedRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Restrictive,
		Roles: chschema.RoleSelection{All: true, Except: []string{"admin"}}}
	runtime := rowPolicyConversionRuntime()

	declared, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{
		Target: "clickhouse", From: schemaext.Observed, To: schemaext.Desired, Values: []schemaext.Value{observed},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(declared, qt.DeepEquals, []schemaext.Value{observed.Desired()})
	predicted, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{
		Target: "clickhouse", From: schemaext.Desired, To: schemaext.Observed, Values: declared,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(predicted, qt.DeepEquals, []schemaext.Value{observed})
	declared[0].(*chschema.DesiredRowPolicy).Roles.Except[0] = "mutated"
	c.Assert(observed.Roles.Except, qt.DeepEquals, []string{"admin"})
}
