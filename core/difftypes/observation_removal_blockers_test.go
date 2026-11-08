package difftypes_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
)

// heldBlockers are the removal blockers of a database that holds app.items: a
// stream that names items whole, one that names a column of it, and one each
// on another table and on a table of the same name in another schema.
func heldBlockers() []catalog.RemovalBlocker {
	return []catalog.RemovalBlocker{
		{Schema: "app", Table: "items", Kind: "change_stream", Name: "everything"},
		{Schema: "app", Table: "items", Kind: "change_stream", Name: "labels", Columns: []string{"label"}},
		{Schema: "app", Table: "orders", Kind: "change_stream", Name: "orders"},
		{Schema: "other", Table: "items", Kind: "change_stream", Name: "elsewhere"},
	}
}

// A table's observation holds the blockers the read found naming the table
// itself, under the table's identity, in the read's order, and owns them: a
// change to the read after the capture, or to a clone of it, leaves them as
// they were.
func TestTableObservationCapturesTheTablesRemovalBlockers_HappyPath(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{RemovalBlockers: heldBlockers()}

	observation := difftypes.TableObservationFor(current, catalog.Table{Name: "items", Schema: "app"}, "postgres", identifier.ForDialect("postgres"))

	c.Assert(observation.RemovalBlockers, qt.DeepEquals, heldBlockers()[:2])
	current.RemovalBlockers[1].Columns[0] = "changed"
	c.Assert(observation.RemovalBlockers[1].Columns, qt.DeepEquals, []string{"label"})
	cloned := observation.Clone()
	cloned.RemovalBlockers[1].Columns[0] = "changed"
	c.Assert(observation.RemovalBlockers, qt.DeepEquals, heldBlockers()[:2])
}

// A table nothing names has no blocker, and neither has an observation made
// without a read.
func TestTableObservationWithoutRemovalBlockers_HappyPath(t *testing.T) {
	t.Run("a table nothing names", func(t *testing.T) {
		c := qt.New(t)
		current := &catalog.Database{RemovalBlockers: heldBlockers()}

		observation := difftypes.TableObservationFor(current, catalog.Table{Name: "customers", Schema: "app"}, "postgres", identifier.ForDialect("postgres"))

		c.Assert(observation.RemovalBlockers, qt.IsNil)
	})
	t.Run("no read", func(t *testing.T) {
		c := qt.New(t)

		observation := difftypes.TableObservationFor(nil, catalog.Table{Name: "items", Schema: "app"}, "postgres", identifier.ForDialect("postgres"))

		c.Assert(observation.RemovalBlockers, qt.IsNil)
	})
}
