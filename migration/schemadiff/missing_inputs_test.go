package schemadiff_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

type missingInputRuntime struct {
	*engine.Runtime
	comparisons int
	conversions int
}

func (r *missingInputRuntime) CompareFeatures(context.Context, schemaext.ComparisonRequest) (schemaext.ComparisonResult, error) {
	r.comparisons++
	return schemaext.ComparisonResult{}, errors.New("comparison dispatched with missing input")
}

func (r *missingInputRuntime) ConvertFeatures(context.Context, schemaext.ConversionRequest) ([]schemaext.Value, error) {
	r.conversions++
	return nil, errors.New("conversion dispatched with missing input")
}

func TestComparisonRefusesMissingSnapshotsBeforeDispatch(t *testing.T) {
	for _, test := range []struct {
		name    string
		desired *schemamodel.Database
		current *catalog.Database
	}{
		{name: "both missing"},
		{name: "desired missing", current: &catalog.Database{}},
		{name: "current missing", desired: &schemamodel.Database{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			base, err := builtin.New()
			c.Assert(err, qt.IsNil)
			runtime := &missingInputRuntime{Runtime: base}
			diff, diagnostics, err := schemadiff.CompareReportingUndecidedAdditions(t.Context(), test.desired, test.current, nil, runtime)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(diff, qt.IsNil)
			c.Assert(diagnostics.Empty(), qt.IsTrue)
			diff, err = schemadiff.CompareWithDatabaseInfo(t.Context(), test.desired, test.current, catalog.ServerInfo{Dialect: "postgres"}, nil, runtime)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(diff, qt.IsNil)
			// No connection is needed to refuse an absent snapshot. This also
			// guards the order before catalog-dependent identifier resolution.
			diff, diagnostics, err = schemadiff.CompareWithDatabaseReportingUndecidedAdditions(t.Context(), nil, test.desired, test.current, nil, runtime)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(diff, qt.IsNil)
			c.Assert(diagnostics.Empty(), qt.IsTrue)
			c.Assert(runtime.comparisons, qt.Equals, 0)
			c.Assert(runtime.conversions, qt.Equals, 0)
		})
	}
}

func TestCompareSchemasRefusesMissingDocumentsBeforeConversion(t *testing.T) {
	for _, test := range []struct {
		name             string
		desired, current *schemamodel.Database
	}{
		{name: "both missing"},
		{name: "desired missing", current: &schemamodel.Database{}},
		{name: "current missing", desired: &schemamodel.Database{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			base, err := builtin.New()
			c.Assert(err, qt.IsNil)
			runtime := &missingInputRuntime{Runtime: base}
			diff, err := schemadiff.CompareSchemas(t.Context(), test.desired, test.current, "postgres", runtime)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(diff, qt.IsNil)
			c.Assert(runtime.comparisons, qt.Equals, 0)
			c.Assert(runtime.conversions, qt.Equals, 0)
		})
	}
}

func TestComparisonAcceptsExplicitEmptySnapshots(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDialect(t.Context(), &schemamodel.Database{}, &catalog.Database{}, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
	diff, err = schemadiff.CompareSchemas(t.Context(), &schemamodel.Database{}, &schemamodel.Database{}, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}
