package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

// ordersTable is the table every compared reference is below.
func ordersTable() objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("", "orders")
}

// keptObjects holds one object of the first kind, named kept, below orders.
func keptObjects(c *qt.C) schemaext.Objects {
	c.Helper()
	objects, err := schemaext.NewObjects(schemaext.Object{Ref: comparedRef(conversionFirst, "kept"), Value: &conversionValue{ID: conversionFirst, Number: 1}})
	c.Assert(err, qt.IsNil)
	return objects
}

// changing replies with one change for the object of the first kind named
// subject.
func changing(subject string) comparisonFunc {
	return func(_ context.Context, r schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
		return schemaext.ObjectComparisonResult{Complete: true, Desired: r.Desired,
			Changes: []schemaext.ChangeRecord{{Subject: comparedRef(conversionFirst, subject), Value: &comparedChange{Number: 1}}}}, nil
	}
}

// TestObjectComparison_OutlivingObjectsAreComparedWithoutTheirTable_HappyPath
// hands the service an object the current side holds below a table it does
// not hold, when the comparison declares that its objects outlive their
// table, and takes the service's change for it as the object's own: with no
// table of the name on either side, and with one the plan creates.
func TestObjectComparison_OutlivingObjectsAreComparedWithoutTheirTable_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name    string
		parents []schemaext.ParentState
	}{
		{name: "no table of the name on either side"},
		{name: "a table the plan creates", parents: []schemaext.ParentState{{Subject: ordersTable(), Desired: true}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var received schemaext.ObjectComparisonRequest
			provider := comparisonProvider(comparisonFunc(func(ctx context.Context, r schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
				received = r
				return changing("kept")(ctx, r)
			}))
			provider.Comparisons[0].OutlivesParent = true
			runtime := mustRuntime(c, provider)

			result, err := runtime.CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{Target: "custom",
				Current: schemaext.ObjectState{Objects: keptObjects(c)}, Parents: test.parents})

			c.Assert(err, qt.IsNil)
			c.Assert(received.Current.Objects.Len(), qt.Equals, 1)
			c.Assert(result.Changes, qt.HasLen, 1)
			c.Assert(result.Changes[0].Subject.Key(), qt.Equals, comparedRef(conversionFirst, "kept").Key())
		})
	}
}

// TestObjectComparison_OutlivingRecordsAreReportedWithoutTheirTable reports
// as undecided an object the current side could not describe below a table
// it does not hold: the record is the subject, as the object would be.
func TestObjectComparison_OutlivingRecordsAreReportedWithoutTheirTable(t *testing.T) {
	c := qt.New(t)
	subject := comparedRef(conversionFirst, "undescribed")
	diagnostic := schemaext.UndecidedChange{Kind: conversionFirst, Subject: subject, Reason: "on a relation the read does not hold"}
	provider := comparisonProvider(comparisonFunc(func(_ context.Context, r schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
		return schemaext.ObjectComparisonResult{Complete: true, Desired: r.Desired, Undecided: []schemaext.UndecidedChange{diagnostic}}, nil
	}))
	provider.Comparisons[0].OutlivesParent = true
	runtime := mustRuntime(c, provider)
	current := namespaceState(c, runtime.Codecs(), schemaext.Observed, schemaext.Complete)
	var err error
	current.Coverage, err = schemaext.NewCoverage(schemaext.Observed, current.Coverage.KindRecords(), []schemaext.SubjectCoverage{
		{Kind: conversionFirst, Subject: subject, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: diagnostic.Reason}}})
	c.Assert(err, qt.IsNil)

	result, err := runtime.CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{Target: "custom", Kinds: []schemaext.Kind{conversionFirst}, Current: current})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Undecided, qt.DeepEquals, []schemaext.UndecidedChange{diagnostic})
}

// TestObjectComparison_OutlivingObjectsAreComparedWithoutTheirTable_FailurePath
// keeps every other refusal: a comparison that does not declare that its
// objects outlive their table is refused an observed object below a missing
// table, a declared object still needs its table, a change for an object
// below a table the plan drops still belongs to that table's transition, and
// a change for an object neither side holds is still unrelated.
func TestObjectComparison_OutlivingObjectsAreComparedWithoutTheirTable_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name     string
		outlives bool
		declared []string
		parents  []schemaext.ParentState
		changed  string
		want     string
	}{
		{name: "a comparison whose objects do not outlive their table", changed: "kept",
			want: `.*feature object has no parent on its source side: .*kept.*`},
		{name: "a declared object below a missing table", outlives: true, declared: []string{"kept"}, changed: "kept",
			want: `.*feature object has no parent on its source side: .*kept.*`},
		{name: "a change below a table the plan drops", outlives: true, changed: "kept",
			parents: []schemaext.ParentState{{Subject: ordersTable(), Current: true}},
			want:    `.*parent transition already owns change .*kept.*`},
		{name: "a change for an object neither side holds", outlives: true, changed: "invented",
			want: `.*comparison returned an unrelated or duplicate change for .*invented.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			provider := comparisonProvider(changing(test.changed))
			provider.Comparisons[0].OutlivesParent = test.outlives
			runtime := mustRuntime(c, provider)
			var declared []schemaext.Object
			for _, name := range test.declared {
				declared = append(declared, schemaext.Object{Ref: comparedRef(conversionFirst, name), Value: &conversionValue{ID: conversionFirst, Number: 1}})
			}
			desired, err := schemaext.NewObjects(declared...)
			c.Assert(err, qt.IsNil)
			request := schemaext.ObjectComparisonRequest{Target: "custom", Desired: schemaext.ObjectState{Objects: desired},
				Current: schemaext.ObjectState{Objects: keptObjects(c)}, Parents: test.parents}

			result, err := runtime.CompareObjects(t.Context(), request)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}
