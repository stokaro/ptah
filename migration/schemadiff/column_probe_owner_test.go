package schemadiff_test

import (
	"context"
	"errors"
	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	"ptah.run/core/renderer"
	"ptah.run/core/schemacomparison"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
	"testing"
)

type columnProbeRuntime struct{ *selectedProbeRuntime }

func (columnProbeRuntime) ComparisonRules(string) (schemacomparison.Rules, error) {
	return schemacomparison.Rules{ColumnService: uninvokedColumns{}}, nil
}

// Validation stops before the offline batch. This sentinel proves only the
// live probe boundary and does not provide a second implementation of columns.
type uninvokedColumns struct{ schemacomparison.ColumnService }

func TestDatabaseComparisonSkipsSpellingProbesForColumnOwner(t *testing.T) {
	c := qt.New(t)
	desired, current := probeComparisonSchemas()
	desired.Functions, current.Functions = nil, nil
	desired.Fields[1].DefaultExpr = "owned_default()"
	stop := errors.New("validation reached")
	embedded := must.Must(builtin.New())
	rendered := 0
	service := probeRenderFunc(func(context.Context, renderer.Request) (renderer.Result, error) {
		rendered++
		return renderer.Result{}, errors.New("unexpected common probe")
	})
	runtime := columnProbeRuntime{&selectedProbeRuntime{TargetRuntime: embedded, NormalizationService: embedded, FacetNormalizationService: embedded, service: service, stop: stop}}
	diff, err := schemadiff.CompareWithDatabase(c.Context(), probeComparisonConnection(t), desired, current, nil, runtime)
	c.Assert(err, qt.ErrorIs, stop)
	c.Assert(diff, qt.IsNil)
	c.Assert(rendered, qt.Equals, 0)
	c.Assert(runtime.validations, qt.Equals, 1)
}
