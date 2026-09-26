package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// TestTriggerCanonicalize spells a trigger the way PostgreSQL 18.6 reports it
// where the spelling carries no meaning, and leaves alone what does: the
// events go in the server's order, and an UPDATE's columns keep the order and
// case they were written in, because `"Total"` and `total` are two columns.
func TestTriggerCanonicalize(t *testing.T) {
	c := qt.New(t)

	trigger := schemamodel.Trigger{
		Timing:   " before ",
		Event:    `update of "Total", b or insert`,
		When:     "  NEW.a > 0 ",
		OldTable: " o ",
		NewTable: " n ",
	}
	trigger.Canonicalize()

	c.Assert(trigger, qt.DeepEquals, schemamodel.Trigger{
		Timing:   "BEFORE",
		Event:    `INSERT OR UPDATE OF "Total", b`,
		ForEach:  "ROW",
		When:     "NEW.a > 0",
		OldTable: "o",
		NewTable: "n",
	})
}

// TestCanonicalTriggerEvent is the order PostgreSQL 18.6 prints a trigger's
// events in, whatever order they were declared in.
func TestCanonicalTriggerEvent(t *testing.T) {
	c := qt.New(t)
	c.Assert(schemamodel.CanonicalTriggerEvent("TRUNCATE OR UPDATE OF b, a OR DELETE OR INSERT"),
		qt.Equals, "INSERT OR DELETE OR UPDATE OF b, a OR TRUNCATE")
}
