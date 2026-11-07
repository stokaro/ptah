package providerhost

// White-box testing required: the index rules are private adapter state the
// shared comparison asks, and they decide whether a changed key NULLS order
// drops and builds an index or is reported synced. The subprocess tests in
// integration/providercli cover the engine.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
)

// The rules of a provider that states NULLS orders compare each key part's
// order exactly, as the preparation states the declared one and a read the
// held one; without the feature they compare none.
func TestObservedIndexRulesCompareNullsOrders(t *testing.T) {
	held := catalog.Index{Name: "i", Columns: []string{"a", "b"}, Parts: []catalog.IndexPart{{Name: "a"}, {Name: "b", Desc: true, NullsOrder: catalog.NullsOrderFirst}}}
	for _, row := range []struct {
		name  string
		rules observedIndexRules
		parts []schemamodel.IndexPart
		equal bool
	}{
		{"the held orders", observedIndexRules{nulls: true}, []schemamodel.IndexPart{{Name: "a"}, {Name: "b", Desc: true, NullsOrder: schemamodel.NullsOrderFirst}}, true},
		{"another order of one part", observedIndexRules{nulls: true}, []schemamodel.IndexPart{{Name: "a"}, {Name: "b", Desc: true}}, false},
		{"an order where the held part has the default", observedIndexRules{nulls: true},
			[]schemamodel.IndexPart{{Name: "a", NullsOrder: schemamodel.NullsOrderLast}, {Name: "b", Desc: true, NullsOrder: schemamodel.NullsOrderFirst}}, false},
		{"no stated orders", observedIndexRules{nulls: true}, nil, true},
		{"a key of another length", observedIndexRules{nulls: true}, []schemamodel.IndexPart{{Name: "a", NullsOrder: schemamodel.NullsOrderLast}}, true},
		{"the feature not negotiated", observedIndexRules{}, []schemamodel.IndexPart{{Name: "a"}, {Name: "b", Desc: true}}, true},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(row.rules.OptionsEqual(schemamodel.Index{Name: "i", Fields: []string{"a", "b"}, Parts: row.parts}, held), qt.Equals, row.equal)
		})
	}
}

// A held index read with columns alone holds each key part in the default
// order.
func TestObservedIndexRulesReadColumnsInTheDefaultOrder(t *testing.T) {
	c := qt.New(t)
	held := catalog.Index{Name: "i", Columns: []string{"a"}}
	rules := observedIndexRules{nulls: true}
	c.Assert(rules.OptionsEqual(schemamodel.Index{Name: "i", Parts: []schemamodel.IndexPart{{Name: "a"}}}, held), qt.IsTrue)
	c.Assert(rules.OptionsEqual(schemamodel.Index{Name: "i", Parts: []schemamodel.IndexPart{{Name: "a", NullsOrder: schemamodel.NullsOrderLast}}}, held), qt.IsFalse)
}
