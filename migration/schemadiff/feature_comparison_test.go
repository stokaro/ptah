package schemadiff_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

func featureSources(c *qt.C, declared, observed []ydbschema.ChangefeedSpec) (*schemamodel.Database, *catalog.Database) {
	c.Helper()
	var desiredObjects, currentObjects []schemaext.Object
	for _, feed := range declared {
		desiredObjects = append(desiredObjects, ydbschema.DesiredObject("", "orders", feed))
	}
	for _, feed := range observed {
		currentObjects = append(currentObjects, ydbschema.ObservedObject("", "orders", feed))
	}
	desired := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "orders", StructName: "Orders"}}}
	current := &catalog.Database{Tables: []catalog.Table{{Name: "orders"}}}
	var err error
	desired.FeatureObjects, err = schemaext.NewObjects(desiredObjects...)
	c.Assert(err, qt.IsNil)
	current.FeatureObjects, err = schemaext.NewObjects(currentObjects...)
	c.Assert(err, qt.IsNil)
	desired.FeatureCoverage, err = ydbschema.ChangefeedCoverage(schemaext.Desired, nil)
	c.Assert(err, qt.IsNil)
	current.FeatureCoverage, err = ydbschema.ChangefeedCoverage(schemaext.Observed, nil)
	c.Assert(err, qt.IsNil)
	return desired, current
}

func TestFeatureComparison_AttachesIndividualChangesAndBothCaptures(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	before := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	after := before.Clone()
	after.Mode = "NEW_IMAGE"
	desired, current := featureSources(c, []ydbschema.ChangefeedSpec{after}, []ydbschema.ChangefeedSpec{before})
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "ydb", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsTrue)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.FeatureChanges, qt.HasLen, 0)
	table := diff.TablesModified[0]
	c.Assert(table.FeatureChanges, qt.HasLen, 1)
	c.Assert(table.FeatureChanges[0].Value, qt.DeepEquals, &ydbdiff.Changefeed{Before: &ydbschema.ObservedChangefeed{Spec: before}, After: &ydbschema.DesiredChangefeed{Spec: after}})
	c.Assert(table.Desired.OwnedObjects, qt.DeepEquals, desired.FeatureObjects)
	c.Assert(table.Current.OwnedObjects, qt.DeepEquals, current.FeatureObjects)
	c.Assert(table.Current.FeatureCoverage, qt.DeepEquals, current.FeatureCoverage)
}

func TestFeatureComparison_AdoptsBeforeCapturingARebuild(t *testing.T) {
	held := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON", Disabled: true}
	keys := ydbschema.ChangefeedSpec{Name: "keys", Mode: "KEYS_ONLY", Format: "JSON"}
	redeclared := held.Clone()
	redeclared.Mode = "NEW_IMAGE"
	redeclared.Disabled = false
	cases := []struct {
		name     string
		declared []ydbschema.ChangefeedSpec
		want     []ydbschema.ChangefeedSpec
		changes  int
	}{
		{name: "source cannot describe streams", want: []ydbschema.ChangefeedSpec{keys, held}},
		{name: "explicit stream wins over observed spelling", declared: []ydbschema.ChangefeedSpec{redeclared}, want: []ydbschema.ChangefeedSpec{keys, redeclared}, changes: 1},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			desired, current := featureSources(c, test.declared, []ydbschema.ChangefeedSpec{held, keys})
			desired.FeatureCoverage = schemaext.Coverage{}
			desired.Tables[0].Comment = "forces common table modification"
			before := desired.FeatureObjects
			diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "ydb", runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			captured, err := ydbschema.DesiredChangefeeds(diff.TablesModified[0].Desired.OwnedObjects, "", "orders")
			c.Assert(err, qt.IsNil)
			c.Assert(captured, qt.DeepEquals, test.want)
			c.Assert(diff.TablesModified[0].FeatureChanges, qt.HasLen, test.changes)
			c.Assert(desired.FeatureObjects, qt.DeepEquals, before)
			c.Assert(diff.TablesModified[0].Current.OwnedObjects, qt.DeepEquals, current.FeatureObjects)
		})
	}
}

func TestFeatureComparison_UnknownNamespaceCannotReportSynced(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	desired, current := featureSources(c, nil, nil)
	current.FeatureCoverage = schemaext.Coverage{}
	opts := config.DefaultCompareOptions()
	opts.Dialect = "ydb"
	partial, diagnostics, err := schemadiff.CompareReportingUndecidedAdditions(t.Context(), desired, current, opts, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(partial.HasChanges(), qt.IsFalse)
	c.Assert(diagnostics.Features, qt.HasLen, 1)
	c.Assert(diagnostics.Features[0].Kind, qt.Equals, ydbschema.ChangefeedKind)
	c.Assert(diagnostics.Features[0].Subject.Name.Source, qt.Equals, "orders")
	refused, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "ydb", runtime)
	c.Assert(err, qt.ErrorIs, schemadiff.ErrIncompleteComparison)
	c.Assert(refused, qt.IsNil)
	var incomplete *schemadiff.IncompleteComparisonError
	c.Assert(err, qt.ErrorAs, &incomplete)
	c.Assert(incomplete.Diagnostics.Features, qt.DeepEquals, diagnostics.Features)
}

type failedComparisonRuntime struct {
	*engine.Runtime
	failure error
	cancel  context.CancelFunc
}

func (r failedComparisonRuntime) CompareFeatures(ctx context.Context, request schemaext.ComparisonRequest) (schemaext.ComparisonResult, error) {
	result, err := r.Runtime.CompareFeatures(ctx, request)
	if err != nil {
		return schemaext.ComparisonResult{}, err
	}
	if r.cancel != nil {
		r.cancel()
	}
	return result, r.failure
}

func TestFeatureComparison_DiscardsServiceFailureOrCancellation(t *testing.T) {
	failure := errors.New("provider terminated")
	canceled, cancel := context.WithCancel(t.Context())
	defer cancel()
	for _, test := range []struct {
		name   string
		ctx    context.Context
		fail   error
		cancel context.CancelFunc
		want   error
	}{
		{name: "provider failure", ctx: t.Context(), fail: failure, want: failure},
		{name: "canceled reply", ctx: canceled, cancel: cancel, want: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			selected := &failedComparisonRuntime{Runtime: runtime, failure: test.fail, cancel: test.cancel}
			desired, current := featureSources(c, []ydbschema.ChangefeedSpec{{Name: "updates", Mode: "UPDATES", Format: "JSON"}}, nil)
			opts := config.DefaultCompareOptions()
			opts.Dialect = "ydb"
			diff, diagnostics, err := schemadiff.CompareReportingUndecidedAdditions(test.ctx, desired, current, opts, selected)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(diff, qt.IsNil)
			c.Assert(diagnostics.Empty(), qt.IsTrue)
		})
	}
}

func TestFeatureComparison_LiteralDotsKeepTheCorrectParent(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	before := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	after := before.Clone()
	after.Mode = "NEW_IMAGE"
	desired, current := featureSources(c, nil, nil)
	desired.Tables = []schemamodel.Table{{Name: "tenant.orders", StructName: "Literal"}, {Schema: "tenant", Name: "orders", StructName: "Qualified"}}
	current.Tables = []catalog.Table{{Name: "tenant.orders"}, {Schema: "tenant", Name: "orders"}}
	desired.FeatureObjects, err = schemaext.NewObjects(ydbschema.DesiredObject("", "tenant.orders", after), ydbschema.DesiredObject("tenant", "orders", before))
	c.Assert(err, qt.IsNil)
	current.FeatureObjects, err = schemaext.NewObjects(ydbschema.ObservedObject("", "tenant.orders", before), ydbschema.ObservedObject("tenant", "orders", before))
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "ydb", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	changed := diff.TablesModified[0]
	c.Assert(changed.Desired.Table.StructName, qt.Equals, "Literal")
	c.Assert(changed.Current.Table.Schema, qt.Equals, "")
	c.Assert(changed.FeatureChanges[0].Subject.Parent.Source, qt.Equals, "tenant.orders")
	c.Assert(changed.Desired.OwnedObjects.Len(), qt.Equals, 1)
	c.Assert(changed.Current.OwnedObjects.Len(), qt.Equals, 1)
}

func TestFeatureComparison_CapturesConstraintOnlyOperands(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	stream := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	desired, current := featureSources(c, []ydbschema.ChangefeedSpec{stream}, []ydbschema.ChangefeedSpec{stream})
	desired.Constraints = []schemamodel.Constraint{{StructName: "Orders", Name: "orders_pk", Type: "PRIMARY KEY", Columns: []string{"id"}}}
	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, "ydb", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesModified, qt.HasLen, 0)
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 1)
	c.Assert(diff.DeclaredConstraintHosts, qt.HasLen, 1)
	c.Assert(diff.ObservedConstraintHosts, qt.HasLen, 1)
	c.Assert(diff.DeclaredConstraintHosts[0].OwnedObjects, qt.DeepEquals, desired.FeatureObjects)
	c.Assert(diff.ObservedConstraintHosts[0].OwnedObjects, qt.DeepEquals, current.FeatureObjects)
	c.Assert(diff.ObservedConstraintHosts[0].FeatureCoverage, qt.DeepEquals, current.FeatureCoverage)
}
