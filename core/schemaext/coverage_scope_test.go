package schemaext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

func TestCoverage_ParentNamespaceDoesNotHideSubjectLimits(t *testing.T) {
	c := qt.New(t)
	registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired)))
	c.Assert(err, qt.IsNil)
	child := widgetRef("orders.2024", "updates")
	parent := objectidentity.ID{Kind: objectidentity.KindTable, Catalog: child.Catalog, Schema: child.Schema, Name: child.Parent}
	limit := schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unknown child field"}
	coverage, err := schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{Model: registry.Definitions()[0], Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "source has no feature syntax"}}}, []schemaext.SubjectCoverage{
		{Kind: widgetKind, Subject: parent, Knowledge: schemaext.Knowledge{State: schemaext.Complete}},
		{Kind: widgetKind, Subject: child, Knowledge: limit},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(coverage.Lookup(widgetKind, child), qt.Equals, limit)
	c.Assert(coverage.Lookup(widgetKind, widgetRef("orders.2024", "other")).State, qt.Equals, schemaext.Complete)
	c.Assert(coverage.Lookup(widgetKind, widgetRef("orders", "2024.other")).State, qt.Equals, schemaext.Uninspected)
	_, explicit := coverage.SubjectKnowledge(widgetKind, widgetRef("orders.2024", "other"))
	c.Assert(explicit, qt.IsFalse)
	captured := coverage.ForParent(parent)
	c.Assert(captured.SubjectRecords(), qt.DeepEquals, coverage.SubjectRecords())
	c.Assert(captured.Lookup(widgetKind, child), qt.Equals, limit)
	other := parent
	other.Name = widgetRef("orders", "x").Parent
	c.Assert(coverage.ForParent(other).SubjectRecords(), qt.HasLen, 0)
	c.Assert(coverage.ForParent(other).KindRecords(), qt.DeepEquals, coverage.KindRecords())
	selected := coverage.SelectSubjects(func(ref objectidentity.ID) bool { return ref.Key() == child.Key() })
	c.Assert(selected.Representation(), qt.Equals, schemaext.Desired)
	c.Assert(selected.Lookup(widgetKind, child), qt.Equals, limit)
	// Removing the parent record must expose the original unknown namespace,
	// not manufacture complete knowledge for other children.
	c.Assert(selected.Lookup(widgetKind, widgetRef("orders.2024", "other")).State, qt.Equals, schemaext.Uninspected)
	c.Assert(coverage.Lookup(widgetKind, widgetRef("orders.2024", "other")).State, qt.Equals, schemaext.Complete)
	c.Assert(coverage.SelectSubjects(nil).SubjectRecords(), qt.DeepEquals, coverage.SubjectRecords())
	c.Assert((schemaext.Coverage{}).SelectSubjects(nil).Representation(), qt.Equals, schemaext.Representation(""))
}

func TestCoverage_DisjointDispatchDiffersFromIndependentSources(t *testing.T) {
	c := qt.New(t)
	registry, err := schemaext.NewRegistry(owned(widgetCodec(widgetKind, schemaext.Desired)), owned(widgetCodec(otherKind, schemaext.Desired)))
	c.Assert(err, qt.IsNil)
	var records []schemaext.KindCoverage
	for _, model := range registry.Definitions() {
		records = append(records, schemaext.KindCoverage{Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
	}
	source, err := schemaext.NewCoverage(schemaext.Desired, records, nil)
	c.Assert(err, qt.IsNil)
	left, right := source.SelectKinds([]schemaext.Kind{widgetKind}), source.SelectKinds([]schemaext.Kind{otherKind})
	combined, err := left.Combine(right)
	c.Assert(err, qt.IsNil)
	c.Assert(combined.KindRecords(), qt.DeepEquals, source.KindRecords())
	_, err = left.Combine(left)
	c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	merged, err := left.Merge(right)
	c.Assert(err, qt.IsNil)
	c.Assert(merged.Lookup(widgetKind, widgetRef("orders", "x")).State, qt.Equals, schemaext.Uninspected)
	c.Assert(merged.Lookup(otherKind, widgetRef("orders", "x")).State, qt.Equals, schemaext.Uninspected)
}
