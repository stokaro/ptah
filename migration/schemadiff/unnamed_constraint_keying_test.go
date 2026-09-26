package schemadiff_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// unnamedChecksDesired declares table d with two CHECKs and no name for either,
// the model a SQL file read for MySQL or SQLite produces and one an embedder can
// build for any dialect.
func unnamedChecksDesired(checks ...string) *schemamodel.Database {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "D", Name: "d"}},
		Fields: []schemamodel.Field{
			{StructName: "D", Name: "lo", Type: "INTEGER", Nullable: true},
			{StructName: "D", Name: "hi", Type: "INTEGER", Nullable: true},
		},
	}
	for _, check := range checks {
		desired.Constraints = append(desired.Constraints, schemamodel.Constraint{
			StructName: "D", Table: "d", Type: "CHECK", CheckExpression: check,
		})
	}
	return desired
}

// namedChecksCurrent is table d holding CHECKs under the names a server gave
// them.
func namedChecksCurrent(names, clauses []string) *catalog.Database {
	current := &catalog.Database{
		Tables: []catalog.Table{{Name: "d", Columns: []catalog.Column{
			{Name: "lo", DataType: "integer", IsNullable: "YES"},
			{Name: "hi", DataType: "integer", IsNullable: "YES"},
		}}},
	}
	for i, name := range names {
		current.Constraints = append(current.Constraints, catalog.Constraint{
			Name: name, TableName: "d", Type: "CHECK", CheckClause: new(clauses[i]),
		})
	}
	return current
}

// TestCompare_EveryUnnamedCheckIsPlanned plans a table whose two CHECKs carry
// no name against a database holding the same two under the names its server
// gave them. Keyed by the empty name, the two would be one: the plan would drop
// both of the server's CHECKs and add back only the later one, and a declared
// CHECK would be gone after apply (stokaro/ptah#3729).
//
// The plan is asserted, not the diff: a CHECK the planner never renders is lost
// as surely as one the comparison never reports.
func TestCompare_EveryUnnamedCheckIsPlanned(t *testing.T) {
	tests := []struct {
		dialect string
		names   []string
		clauses []string
	}{
		{
			dialect: platform.Postgres,
			names:   []string{"d_check", "d_hi_check"},
			clauses: []string{"((lo < hi))", "((hi > 0))"},
		},
		{
			dialect: platform.MySQL,
			names:   []string{"d_chk_1", "d_chk_2"},
			clauses: []string{"(`lo` < `hi`)", "(`hi` > 0)"},
		},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			diff := schemadiff.CompareWithDialect(
				unnamedChecksDesired("lo < hi", "hi > 0"),
				namedChecksCurrent(test.names, test.clauses),
				test.dialect,
			)

			statements, err := planner.GenerateSchemaDiffSQLStatements(diff, test.dialect)

			c.Assert(err, qt.IsNil)
			plan := strings.Join(statements, "\n")
			c.Assert(plan, qt.Contains, "CHECK (lo < hi)")
			c.Assert(plan, qt.Contains, "CHECK (hi > 0)")
		})
	}
}

// TestCompareSchemas_AnUnnamedCheckPairsWithItself compares a desired state
// with itself where a table declares two unnamed CHECKs: nothing is planned.
//
// Both sides of `schema diff` are desired states, so both carry the unnamed
// CHECKs, and each pairs with its copy by its condition. Keyed by the empty
// name, each side would keep one of the two -- the desired side the later, the
// other side the earlier -- and the comparison would plan a CHECK nobody had
// changed.
func TestCompareSchemas_AnUnnamedCheckPairsWithItself(t *testing.T) {
	for _, dialect := range []string{platform.Postgres, platform.MySQL, platform.SQLite} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareSchemas(
				unnamedChecksDesired("lo < hi", "hi > 0"),
				unnamedChecksDesired("lo < hi", "hi > 0"),
				dialect,
			)

			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
		})
	}
}

// TestCompareSchemas_AnUnnamedCheckThatChangedIsPlanned is the control for the
// test above: pairing by condition must still see a condition that changed.
func TestCompareSchemas_AnUnnamedCheckThatChangedIsPlanned(t *testing.T) {
	c := qt.New(t)

	diff := schemadiff.CompareSchemas(
		unnamedChecksDesired("lo < hi", "hi > 0"),
		unnamedChecksDesired("lo <= hi", "hi > 0"),
		platform.Postgres,
	)

	c.Assert(diff.ConstraintsAdded, qt.HasLen, 1)
	c.Assert(diff.ConstraintsAdded[0].CheckExpression, qt.Equals, "lo < hi")
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 1)
	c.Assert(diff.ConstraintsRemoved[0].TableName, qt.Equals, "d")
}

// TestCompare_UnnamedChecksAreOrderedByCondition pins the order of unnamed
// CHECK additions. They share a table and an empty name; ordered by those
// alone, the map the comparison iterates would decide their order, and two runs
// over one schema could write two different plans.
func TestCompare_UnnamedChecksAreOrderedByCondition(t *testing.T) {
	c := qt.New(t)
	desired := unnamedChecksDesired("lo < hi", "hi > 0", "lo > 0")

	for i := range 100 {
		diff := schemadiff.CompareWithDialect(desired, namedChecksCurrent(nil, nil), platform.Postgres)

		conditions := make([]string, 0, len(diff.ConstraintsAdded))
		for _, added := range diff.ConstraintsAdded {
			conditions = append(conditions, added.CheckExpression)
		}
		c.Assert(conditions, qt.DeepEquals, []string{"hi > 0", "lo < hi", "lo > 0"}, qt.Commentf("iteration %d", i))
	}
}
