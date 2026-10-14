package cockroachcomparison_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/cockroachcomparison"
)

// TestIndexes_OptionsEqual reads a declared access method against the one
// CockroachDB reports. The reader reports btree and gin, and CockroachDB prints
// a GIST index as gin too, so those spellings are one method.
func TestIndexes_OptionsEqual(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		reported string
		want     bool
	}{
		{name: "no method is btree", declared: "", reported: "btree", want: true},
		{name: "BTREE in any case", declared: "BTREE", reported: "btree", want: true},
		{name: "GIN", declared: "GIN", reported: "gin", want: true},
		{name: "GIST, which CockroachDB builds as GIN", declared: "GIST", reported: "gin", want: true},
		{name: "btree against gin", declared: "", reported: "gin", want: false},
		{name: "gin against btree", declared: "GIN", reported: "btree", want: false},
		{name: "a method CockroachDB does not report", declared: "HASH", reported: "btree", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := cockroachcomparison.Indexes{}.OptionsEqual(
				schemamodel.Index{Type: test.declared},
				catalog.Index{Method: test.reported},
			)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestIndexes_RenameCarries keeps every facet through a rename, because
// ALTER INDEX ... RENAME TO changes the name alone.
func TestIndexes_RenameCarries(t *testing.T) {
	c := qt.New(t)

	c.Assert(cockroachcomparison.Indexes{}.RenameCarries(schemaext.Kind("any.index.facet")), qt.IsTrue)
}

// TestRules_RegistersIndexesAlone is the shape the built-in runtime installs:
// index rules and nothing else, so the other comparisons stay shared.
func TestRules_RegistersIndexesAlone(t *testing.T) {
	c := qt.New(t)

	rules := cockroachcomparison.Rules()

	c.Assert(rules.Indexes, qt.Equals, cockroachcomparison.Indexes{})
	c.Assert(rules.Columns, qt.IsNil)
	c.Assert(rules.Views, qt.IsNil)
	c.Assert(rules.Routines, qt.IsNil)
}
