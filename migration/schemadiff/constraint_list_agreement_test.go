package schemadiff_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestCompareWithDialect_TheConstraintListsCarryTheNamesTheirFixturesExpect
// pins the names and the multiplicity the one constraint list carries.
//
// It asked a different question until the second list went: there were two, a
// list of names and a list of records, and whether they could disagree decided
// whether either could be retired -- because migration/safety classifies a diff
// by the LENGTH of the list (stokaro/ptah#1663). They could not, so the name
// list was retired (stokaro/ptah#2315).
//
// What survives the retirement is the property that made it safe: a name
// repeats once per HOST, so the length is the number of objects rather than the
// number of distinct names, and the safety verdict is unchanged.
//
// The multiplicity matters and is why this asserts a sorted list rather than a
// set: one name on two tables is two constraints, and a modify contributes the
// same name to added and removed at once.
func TestCompareWithDialect_TheConstraintListsCarryTheNamesTheirFixturesExpect(t *testing.T) {
	rows := []struct {
		name     string
		dialect  string
		desired  func() *schemamodel.Database
		current  func() *catalog.Database
		wantAdds []string
		wantDrop []string
	}{
		{
			name:     "a constraint the target does not have",
			dialect:  "postgres",
			desired:  constraintSchema("orders", "orders_total_positive"),
			current:  constraintDatabase(),
			wantAdds: []string{"orders_total_positive"},
			wantDrop: nil,
		},
		{
			name:     "a constraint the desired schema no longer declares",
			dialect:  "postgres",
			desired:  constraintSchema("orders"),
			current:  constraintDatabase(dbConstraint("orders", "orders_total_positive", "total > 0")),
			wantAdds: nil,
			wantDrop: []string{"orders_total_positive"},
		},
		{
			name:     "a constraint whose definition changed",
			dialect:  "postgres",
			desired:  constraintSchema("orders", "orders_total_positive"),
			current:  constraintDatabase(dbConstraint("orders", "orders_total_positive", "total > 100")),
			wantAdds: []string{"orders_total_positive"},
			wantDrop: []string{"orders_total_positive"},
		},
		{
			name:    "one name on two tables",
			dialect: "postgres",
			desired: constraintSchema("orders", "orders_total_positive"),
			current: constraintDatabase(
				dbConstraint("orders", "shared_name", "total > 0"),
				dbConstraint("invoices", "shared_name", "total > 0"),
			),
			wantAdds: []string{"orders_total_positive"},
			wantDrop: []string{"shared_name", "shared_name"},
		},
		{
			name:     "the same shapes on mysql, whose identity folds differently",
			dialect:  "mysql",
			desired:  constraintSchema("orders", "orders_total_positive"),
			current:  constraintDatabase(dbConstraint("orders", "ORDERS_TOTAL_POSITIVE", "total > 100")),
			wantAdds: []string{"orders_total_positive"},
			wantDrop: []string{"ORDERS_TOTAL_POSITIVE"},
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(row.desired(), row.current(), row.dialect)

			c.Assert(sortedCopy(diff.ConstraintsAdded.Names()), qt.DeepEquals, sortedCopy(row.wantAdds),
				qt.Commentf("the fixture did not produce the additions it was written for"))
			c.Assert(sortedCopy(diff.ConstraintsRemoved.Names()), qt.DeepEquals, sortedCopy(row.wantDrop),
				qt.Commentf("the fixture did not produce the removals it was written for"))
			c.Assert(diff.ConstraintsAdded, qt.HasLen, len(row.wantAdds),
				qt.Commentf("one entry per host, which is what migration/safety counts"))
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, len(row.wantDrop))
		})
	}
}

// constraintSchema builds a desired schema with one table and the named CHECK
// constraints on it.
func constraintSchema(table string, constraints ...string) func() *schemamodel.Database {
	return func() *schemamodel.Database {
		schema := &schemamodel.Database{
			Tables: []schemamodel.Table{{StructName: "Order", Name: table}},
			Fields: []schemamodel.Field{
				{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true},
				{StructName: "Order", Name: "total", Type: "INTEGER", Nullable: false},
			},
		}
		for _, name := range constraints {
			schema.Constraints = append(schema.Constraints, schemamodel.Constraint{
				StructName:      "Order",
				Name:            name,
				Table:           table,
				Type:            "CHECK",
				CheckExpression: "total > 0",
			})
		}
		return schema
	}
}

// dbConstraint is one introspected CHECK constraint.
func dbConstraint(table, name, expression string) catalog.Constraint {
	return catalog.Constraint{
		Name:        name,
		TableName:   table,
		Type:        "CHECK",
		CheckClause: &expression,
	}
}

// constraintDatabase builds an introspected schema holding the two tables the
// fixtures name and the constraints given.
func constraintDatabase(constraints ...catalog.Constraint) func() *catalog.Database {
	return func() *catalog.Database {
		table := func(name string) catalog.Table {
			return catalog.Table{
				Name: name,
				Type: "TABLE",
				Columns: []catalog.Column{
					{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
					{Name: "total", DataType: "integer", IsNullable: "NO"},
				},
			}
		}
		return &catalog.Database{
			Tables:      []catalog.Table{table("orders"), table("invoices")},
			Constraints: slices.Clone(constraints),
		}
	}
}

// sortedCopy sorts a copy, so an assertion cannot reorder the diff it reads.
func sortedCopy(names []string) []string {
	if len(names) == 0 {
		return make([]string, 0)
	}
	return slices.Sorted(slices.Values(names))
}
