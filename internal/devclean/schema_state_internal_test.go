package devclean

// White-box testing required: a Baseline's realm and schema states are set only
// by Claim, which needs a live server, and this asks each combination of them,
// including one no reachable server produces on demand: a realm claim on an
// engine whose catalog answers no schema states.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbreset"
)

func TestBaseline_ReturnsSchemaState(t *testing.T) {
	states := []dbreset.SchemaState{{Name: "public"}}
	tests := []struct {
		name     string
		baseline Baseline
		want     bool
	}{
		{name: "a realm cleanup with the states recorded", baseline: Baseline{realm: true, schemaStates: states}, want: true},
		{name: "a realm cleanup on an engine that answers none", baseline: Baseline{realm: true}, want: false},
		{name: "a cleanup of a pinned schema, which reads none", baseline: Baseline{schemaStates: states}, want: false},
		{name: "the zero baseline", baseline: Baseline{}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.baseline.ReturnsSchemaState(), qt.Equals, test.want)
		})
	}
}
