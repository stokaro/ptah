package difftypes_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
)

// TestSchemaDiff_IndexesTakenOver_HappyPath pins that every kind of addition counts: an
// index, a key constraint and a column's own key each name what they take over from, and
// two that name one index count it once.
func TestSchemaDiff_IndexesTakenOver_HappyPath(t *testing.T) {
	c := qt.New(t)
	built := difftypes.IndexRef{Name: "c_fk", TableName: "child"}
	other := difftypes.IndexRef{Name: "d_fk", TableName: "child"}
	unique := difftypes.IndexRef{Name: "old_key", TableName: "child"}
	diff := &difftypes.SchemaDiff{
		IndexesAdded: difftypes.IndexChanges{{
			Index: schemamodel.Index{Name: "w", Fields: []string{"pid", "qid"}}, TableName: "child",
			Replaces: []difftypes.IndexRef{built, unique},
		}},
		ConstraintsAdded: difftypes.ConstraintAdditions{{
			Name: "u", TableName: "child", Type: "UNIQUE", Columns: []string{"pid"},
			Replaces: []difftypes.IndexRef{built},
		}},
		TablesModified: []difftypes.TableDiff{{
			TableName: "child",
			ColumnsModified: []difftypes.ColumnDiff{{
				ColumnName: "rid", Changes: map[string]string{"unique": "false -> true"},
				Replaces: []difftypes.IndexRef{other},
			}},
		}},
	}

	got := diff.IndexesTakenOver()

	c.Assert(got, qt.DeepEquals, map[difftypes.IndexRef]struct{}{built: {}, other: {}, unique: {}})
}

// TestSchemaDiff_IndexesTakenOver_FailurePath pins the empty answer of a diff whose
// additions take over from nothing: an empty map, not nil.
func TestSchemaDiff_IndexesTakenOver_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		IndexesAdded:     difftypes.IndexChanges{{Index: schemamodel.Index{Name: "w"}, TableName: "child"}},
		ConstraintsAdded: difftypes.ConstraintAdditions{{Name: "u", TableName: "child", Type: "UNIQUE"}},
	}

	got := diff.IndexesTakenOver()

	c.Assert(got, qt.IsNotNil)
	c.Assert(got, qt.HasLen, 0)
}

// TestSchemaDiff_TakeoversRideOffTheWire pins that what a key constraint and a column
// change take over from stays out of the JSON shape, as an index addition's does.
func TestSchemaDiff_TakeoversRideOffTheWire(t *testing.T) {
	c := qt.New(t)
	built := []difftypes.IndexRef{{Name: "c_fk", TableName: "child"}}
	addition := difftypes.ConstraintAdditionInfo{Name: "u", TableName: "child", Type: "UNIQUE", Replaces: built}
	column := difftypes.ColumnDiff{ColumnName: "pid", Changes: map[string]string{"unique": "false -> true"}, Replaces: built}

	gotAddition, errAddition := json.Marshal(addition)
	gotColumn, errColumn := json.Marshal(column)

	c.Assert(errAddition, qt.IsNil)
	c.Assert(errColumn, qt.IsNil)
	c.Assert(string(gotAddition), qt.Contains, `"name":"u"`)
	c.Assert(string(gotAddition), qt.Not(qt.Contains), "c_fk")
	c.Assert(string(gotColumn), qt.Contains, `"column_name":"pid"`)
	c.Assert(string(gotColumn), qt.Not(qt.Contains), "c_fk")
}
