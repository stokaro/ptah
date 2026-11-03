package schemascope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/core/selectext"
	"ptah.run/internal/schemascope"
)

// TestFilterScopesViewTriggersOnBothSidesTheSameWay pins that an INSTEAD OF
// trigger rides its view on both sides of a comparison, as a trigger rides its
// table. Scoped by the kept tables alone, the trigger leaves the current side,
// and `migrate diff` plans CREATE TRIGGER again on every run
// (stokaro/ptah#5015). The trigger on a view in a schema the selection leaves
// out leaves with it. The unqualified read stands in for the connected schema,
// which is how the reader names an object there.
func TestFilterScopesViewTriggersOnBothSidesTheSameWay(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Views: []schemamodel.View{{Name: "order_report"}, {Name: "other.w"}},
		Triggers: []schemamodel.Trigger{
			{Name: "order_report_ins", Table: "order_report", Timing: "INSTEAD OF", Event: "INSERT", ForEach: "ROW"},
			{Name: "w_ins", Table: "other.w", Timing: "INSTEAD OF", Event: "INSERT", ForEach: "ROW"},
		},
	}
	database := &catalog.Database{
		Views: []catalog.View{{Name: "order_report"}, {Schema: "other", Name: "w"}},
		Triggers: []catalog.Trigger{
			{Name: "order_report_ins", Table: "order_report", Timing: "INSTEAD OF", Event: "INSERT", ForEach: "ROW"},
			{Name: "w_ins", Schema: "other", Table: "w", Timing: "INSTEAD OF", Event: "INSERT", ForEach: "ROW"},
		},
	}

	generated, err := schemascope.FilterGeneratedWithDefaultSchema(t.Context(), selectext.None(), desired, []string{"public"}, "public")
	c.Assert(err, qt.IsNil)
	read, err := schemascope.FilterDatabaseWithDefaultSchema(t.Context(), selectext.None(), database, []string{"public"}, "public")
	c.Assert(err, qt.IsNil)

	c.Assert(generated.Triggers, qt.DeepEquals, desired.Triggers[:1])
	c.Assert(read.Triggers, qt.DeepEquals, database.Triggers[:1])
}
