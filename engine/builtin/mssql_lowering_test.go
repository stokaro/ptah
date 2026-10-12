package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/engine/builtin"
)

func TestLowerDesired_SQLServerRoutineParameters(t *testing.T) {
	for _, test := range []struct{ target, want string }{{"sqlserver", "@a INT"}, {"postgres", "@a AS INT"}} {
		t.Run(test.target, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Functions: []schemamodel.Function{{Name: "p", Kind: "procedure", Parameters: "@a AS INT"}}}
			result, err := must.Must(builtin.New()).LowerDesired(t.Context(), schemapreparation.LoweringRequest{Target: test.target, Desired: desired})
			c.Assert(err, qt.IsNil)
			c.Assert(result.Functions[0].Parameters, qt.Equals, test.want)
			c.Assert(desired.Functions[0].Parameters, qt.Equals, "@a AS INT")
		})
	}
}
