package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

func TestRemovedSQLiteTablesCaptureOnlyTheirExactChildren(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	current := &catalog.Database{
		Tables:      []catalog.Table{{Name: " docs "}, {Name: "docs"}},
		Indexes:     []catalog.Index{{Name: "spaced_index", TableName: " docs "}, {Name: "plain_index", TableName: "docs"}},
		Constraints: []catalog.Constraint{{Name: "spaced_check", TableName: " docs ", Type: "CHECK", CheckClause: new("1")}, {Name: "plain_check", TableName: "docs", Type: "CHECK", CheckClause: new("1")}},
		Triggers:    []catalog.Trigger{{Name: "spaced_trigger", Table: " docs "}, {Name: "plain_trigger", Table: "docs"}},
	}
	diff, err := schemadiff.CompareWithDialect(t.Context(), &schemamodel.Database{}, current, "sqlite", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesRemoved, qt.HasLen, 2)
	for _, test := range []struct {
		position     int
		name, prefix string
	}{
		{position: 0, name: " docs ", prefix: "spaced"},
		{position: 1, name: "docs", prefix: "plain"},
	} {
		t.Run(test.prefix, func(t *testing.T) {
			c := qt.New(t)
			observed := diff.TablesRemoved[test.position].Current
			c.Assert(observed.Table.Name, qt.Equals, test.name)
			c.Assert(observed.Indexes, qt.HasLen, 1)
			c.Assert(observed.Indexes[0].Name, qt.Equals, test.prefix+"_index")
			c.Assert(observed.Constraints, qt.HasLen, 1)
			c.Assert(observed.Constraints[0].Name, qt.Equals, test.prefix+"_check")
			c.Assert(observed.Triggers, qt.HasLen, 1)
			c.Assert(observed.Triggers[0].Name, qt.Equals, test.prefix+"_trigger")
		})
	}
}
