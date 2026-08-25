package constraintscope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/constraintscope"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestNormalize_EveryBareAdditionGetsARecord pins the property that lets a
// consumer read constraint additions from the records alone.
//
// A [difftypes.SchemaDiff] carries two answers to one question: a list of names
// and a list of records. The comparator fills both, but the type is a surface an
// embedder constructs by hand, and the generator's reverse path fills the names
// for constraints whose body the pre-change schema does not carry. A planner
// that read only records would lose exactly those (stokaro/ptah#1663).
//
// Normalize is the single place that answers it, at the door every planner
// already calls. The record it adds carries no table, which is the state a
// name-only path is for, and the one the planners already read.
//
// Multiplicity is asserted, not membership: one name on two tables is two
// constraints, and a record list holding it once when the name list holds it
// twice is one record short.
func TestNormalize_EveryBareAdditionGetsARecord(t *testing.T) {
	rows := []struct {
		name         string
		diff         *difftypes.SchemaDiff
		wantHostless []string
		wantTotal    int
	}{
		{
			name:         "a name with no record at all",
			diff:         &difftypes.SchemaDiff{ConstraintsAdded: []string{"c"}},
			wantHostless: []string{"c"},
			wantTotal:    1,
		},
		{
			name: "a name whose record carries a table",
			diff: &difftypes.SchemaDiff{
				ConstraintsAdded: []string{"c"},
				ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{
					{Name: "c", TableName: "orders", Type: "CHECK"},
				},
			},
			wantHostless: nil,
			wantTotal:    1,
		},
		{
			name: "one name on two tables, one of them recorded",
			diff: &difftypes.SchemaDiff{
				ConstraintsAdded: []string{"c", "c"},
				ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{
					{Name: "c", TableName: "orders", Type: "CHECK"},
				},
			},
			wantHostless: []string{"c"},
			wantTotal:    2,
		},
		{
			name:         "no additions at all",
			diff:         &difftypes.SchemaDiff{},
			wantHostless: nil,
			wantTotal:    0,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			constraintscope.Normalize(row.diff, identifier.ForDialect("postgres"))

			c.Assert(hostless(row.diff), qt.DeepEquals, row.wantHostless)
			c.Assert(row.diff.ConstraintsAddedWithTables, qt.HasLen, row.wantTotal)
			c.Assert(constraintscope.AdditionNames(row.diff), qt.HasLen, len(row.diff.ConstraintsAdded),
				qt.Commentf("the records do not cover the names"))
		})
	}
}

// TestNormalize_RunningTwiceChangesNothing is the control.
//
// Normalize is called at every planner's door and a diff can pass through more
// than one. A pass that appended a second record for a name it already covered
// would make a planner emit the constraint twice, and the assertions above
// would not see it.
func TestNormalize_RunningTwiceChangesNothing(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		ConstraintsAdded: []string{"c", "d"},
		ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{
			{Name: "c", TableName: "orders", Type: "CHECK"},
		},
		ConstraintsRemoved: []string{"e", "f"},
		ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{
			{Name: "e", TableName: "orders", Type: "CHECK"},
		},
	}

	constraintscope.Normalize(diff, identifier.ForDialect("postgres"))
	onceAdded := append([]difftypes.ConstraintAdditionInfo(nil), diff.ConstraintsAddedWithTables...)
	onceRemoved := append([]difftypes.ConstraintRemovalInfo(nil), diff.ConstraintsRemovedWithTables...)
	constraintscope.Normalize(diff, identifier.ForDialect("postgres"))

	c.Assert(diff.ConstraintsAddedWithTables, qt.DeepEquals, onceAdded)
	c.Assert(diff.ConstraintsRemovedWithTables, qt.DeepEquals, onceRemoved)
}

// hostless lists the additions that name no table, nil when none.
func hostless(diff *difftypes.SchemaDiff) []string {
	var names []string
	for _, info := range diff.ConstraintsAddedWithTables {
		if info.TableName != "" {
			continue
		}
		names = append(names, info.Name)
	}
	return names
}

// TestNormalize_EveryBareRemovalGetsARecord is [TestNormalize_EveryBareAdditionGetsARecord]
// for the other direction.
//
// Both lists have the same shape and the same two producers, so both need the
// same guarantee before a consumer can read records alone. Asserted separately
// rather than folded into one table, because a Normalize that covered one list
// and not the other would pass a combined test that only counted records.
func TestNormalize_EveryBareRemovalGetsARecord(t *testing.T) {
	rows := []struct {
		name         string
		diff         *difftypes.SchemaDiff
		wantHostless []string
		wantTotal    int
	}{
		{
			name:         "a name with no record at all",
			diff:         &difftypes.SchemaDiff{ConstraintsRemoved: []string{"c"}},
			wantHostless: []string{"c"},
			wantTotal:    1,
		},
		{
			name: "a name whose record carries a table",
			diff: &difftypes.SchemaDiff{
				ConstraintsRemoved: []string{"c"},
				ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{
					{Name: "c", TableName: "orders", Type: "CHECK"},
				},
			},
			wantHostless: nil,
			wantTotal:    1,
		},
		{
			name: "one name on two tables, one of them recorded",
			diff: &difftypes.SchemaDiff{
				ConstraintsRemoved: []string{"c", "c"},
				ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{
					{Name: "c", TableName: "orders", Type: "CHECK"},
				},
			},
			wantHostless: []string{"c"},
			wantTotal:    2,
		},
		{
			name:         "no removals at all",
			diff:         &difftypes.SchemaDiff{},
			wantHostless: nil,
			wantTotal:    0,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			constraintscope.Normalize(row.diff, identifier.ForDialect("postgres"))

			c.Assert(hostlessRemovals(row.diff), qt.DeepEquals, row.wantHostless)
			c.Assert(row.diff.ConstraintsRemovedWithTables, qt.HasLen, row.wantTotal)
			c.Assert(constraintscope.RemovalNames(row.diff), qt.HasLen, len(row.diff.ConstraintsRemoved),
				qt.Commentf("the records do not cover the names"))
		})
	}
}

// hostlessRemovals lists the removals that name no table, nil when none.
func hostlessRemovals(diff *difftypes.SchemaDiff) []string {
	var names []string
	for _, info := range diff.ConstraintsRemovedWithTables {
		if info.TableName != "" {
			continue
		}
		names = append(names, info.Name)
	}
	return names
}
