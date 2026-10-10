package featureplan

import (
	"slices"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// CapturedKinds returns, sorted and once each, the feature kinds the table's
// captured state carries on either side: an owned object, a facet on the table
// or on one of its columns, indexes, constraints or triggers, and a coverage
// record for a subject under the table. A table with none carries no state a
// feature owner accounts for; any other needs an owner to account for each kind
// when the table is altered, rebuilt or dropped.
//
// It is the one answer to that question: a host deciding whether to ask the
// runtime about a table, and the runtime checking that every such kind has an
// owner, read the same captured slots. Invalid owned objects return an error.
func (t Table) CapturedKinds() ([]schemaext.Kind, error) {
	declared := schemamodel.Database{Tables: []schemamodel.Table{t.Desired.Table}, Fields: t.Desired.Fields,
		Enums: t.Desired.Enums, Constraints: t.Desired.Constraints, Indexes: t.Desired.Indexes, Triggers: t.Desired.Triggers}
	observed := catalog.Database{Tables: []catalog.Table{t.Current.Table}, Indexes: t.Current.Indexes,
		Constraints: t.Current.Constraints, Triggers: t.Current.Triggers}
	groups := []struct {
		facets []*schemaext.Facets
		state  schemaext.ObjectState
	}{
		{declared.FacetSlots(), schemaext.ObjectState{Objects: t.Desired.OwnedObjects, Coverage: t.Desired.FeatureCoverage}},
		{observed.FacetSlots(), schemaext.ObjectState{Objects: t.Current.OwnedObjects, Coverage: t.Current.FeatureCoverage}},
	}
	var kinds []schemaext.Kind
	for _, group := range groups {
		objects, err := group.state.Objects.All()
		if err != nil {
			return nil, err
		}
		for _, object := range objects {
			kinds = append(kinds, object.Value.Kind())
		}
		for _, facets := range group.facets {
			kinds = append(kinds, facets.Kinds()...)
		}
		for _, record := range group.state.Coverage.ForParent(t.Subject).SubjectRecords() {
			kinds = append(kinds, record.Kind)
		}
	}
	slices.Sort(kinds)
	return slices.Compact(kinds), nil
}
