package providerhost

// White-box testing required: the catalog constraints a read produces and the
// planner's decision whether a UNIQUE change reaches the table's owner or is
// refused are private adapter state. The subprocess tests in
// integration/providercli cover the engine.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// uniquePlanner plans for a provider that owns the table lifecycle, reports
// the foreign keys, CHECKs and UNIQUE constraints its tables hold and plans
// their change.
func uniquePlanner() commonPlanner {
	planner := constraintPlanner()
	planner.observedUnique = true
	return planner
}

// recordsUnique is main.records' UNIQUE over two columns, as the comparison
// adds it.
func recordsUnique() difftypes.ConstraintAdditions {
	return difftypes.ConstraintAdditions{{Name: "records_a_b_key", TableName: "main.records", Type: "UNIQUE", Columns: []string{"a", "b"}}}
}

// A table whose only change is a UNIQUE constraint is altered, so its owner
// compares the declared constraints with the held ones.
func TestOwnedLifecycleUniqueChangeAltersItsTable_HappyPath(t *testing.T) {
	for _, row := range []struct {
		name    string
		added   difftypes.ConstraintAdditions
		removed difftypes.ConstraintRemovals
	}{
		{"a UNIQUE added", recordsUnique(), nil},
		{"a UNIQUE removed", nil, difftypes.ConstraintRemovals{{Name: "records_a_b_key", TableName: "main.records", Type: "UNIQUE"}}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			diff := preparedRecords()
			diff.ConstraintsAdded, diff.ConstraintsRemoved = row.added, row.removed
			_, err := uniquePlanner().commonObjectSteps(diff)
			c.Assert(err, qt.IsNil)
			request, _, err := uniquePlanner().planningContext(diff)
			c.Assert(err, qt.IsNil)
			c.Assert(request.Tables, qt.HasLen, 1)
			c.Assert(request.Tables[0].Action, qt.Equals, featureplan.AlterTable)
		})
	}
}

// A UNIQUE change is refused where the read does not report UNIQUE
// constraints, which leaves the comparison no current side, and by its name
// where the owner does not plan constraint changes.
func TestOwnedLifecycleUniqueChange_FailurePath(t *testing.T) {
	unplanned := uniquePlanner()
	unplanned.constraints = false
	for _, row := range []struct {
		name    string
		planner commonPlanner
		want    string
	}{
		{"the read reports no UNIQUE", constraintPlanner(), `.*does not support SchemaDiff\.ConstraintsAdded`},
		{"the owner does not plan constraint changes", unplanned,
			`.*does not support changing constraint records_a_b_key of table main\.records on the fixture engine, whose provider does not plan it`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			diff := preparedRecords()
			diff.ConstraintsAdded = recordsUnique()
			_, err := row.planner.commonObjectSteps(diff)
			c.Assert(err, qt.ErrorMatches, row.want)
		})
	}
}

// A read that negotiated UNIQUE constraints gives the catalog each one with
// its NULLS [NOT] DISTINCT and deferral, the singular column name the first
// member.
func TestReadKeepsUniqueConstraints_HappyPath(t *testing.T) {
	c := qt.New(t)
	r := reader{session: &session{remote: &protocol.Session{Features: []string{provider.ObservedConstraintsFeature, provider.ObservedUniqueConstraintsFeature}}}}
	constraints := r.constraints(&protocol.Table{Constraints: []*protocol.ObservedConstraint{
		{Name: "posts_a_b_key", TableName: "posts", Schema: "app", Type: "UNIQUE", ColumnNames: []string{"a", "b"}, NullsDistinct: new(false), Deferrable: true,
			Initially: "IMMEDIATE"},
	}})
	c.Assert(constraints, qt.DeepEquals, []catalog.Constraint{
		{Name: "posts_a_b_key", TableName: "posts", Schema: "app", Type: "UNIQUE", ColumnName: "a", ColumnNames: []string{"a", "b"}, NullsDistinct: new(false),
			Deferrable: true, Initially: "IMMEDIATE"},
	})
}
