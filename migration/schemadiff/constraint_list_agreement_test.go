package schemadiff_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestCompareWithDialect_TheBareConstraintListsAgreeWithTheHostedOnes pins that
// the two answers a [difftypes.SchemaDiff] carries about the same question are
// the same answer.
//
// ConstraintsAdded is a list of names and ConstraintsAddedWithTables is a list
// of records; the comparator fills both, and everything downstream pairs them
// by name. Whether they can disagree decides whether one can be retired:
// migration/safety classifies a diff by the LENGTH of the bare lists, so a
// consumer switched to the hosted list would keep the same verdict only if the
// two carry the same names, with the same multiplicity, in every case
// (stokaro/ptah#1663).
//
// The multiplicity matters and is why this asserts a sorted list rather than a
// set: one name on two tables is two constraints, and a modify contributes the
// same name to added and removed at once.
func TestCompareWithDialect_TheBareConstraintListsAgreeWithTheHostedOnes(t *testing.T) {
	rows := []struct {
		name     string
		dialect  string
		desired  func() *goschema.Database
		current  func() *types.DBSchema
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

			c.Assert(sortedCopy(diff.ConstraintsAdded), qt.DeepEquals, sortedCopy(row.wantAdds),
				qt.Commentf("the fixture did not produce the additions it was written for"))
			c.Assert(sortedCopy(diff.ConstraintsRemoved), qt.DeepEquals, sortedCopy(row.wantDrop),
				qt.Commentf("the fixture did not produce the removals it was written for"))
			c.Assert(sortedCopy(diff.ConstraintsAdded), qt.DeepEquals, additionNames(diff.ConstraintsAddedWithTables))
			c.Assert(sortedCopy(diff.ConstraintsRemoved), qt.DeepEquals, removalNames(diff.ConstraintsRemovedWithTables))
		})
	}
}

// constraintSchema builds a desired schema with one table and the named CHECK
// constraints on it.
func constraintSchema(table string, constraints ...string) func() *goschema.Database {
	return func() *goschema.Database {
		schema := &goschema.Database{
			Tables: []goschema.Table{{StructName: "Order", Name: table}},
			Fields: []goschema.Field{
				{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true},
				{StructName: "Order", Name: "total", Type: "INTEGER", Nullable: false},
			},
		}
		for _, name := range constraints {
			schema.Constraints = append(schema.Constraints, goschema.Constraint{
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
func dbConstraint(table, name, expression string) types.DBConstraint {
	return types.DBConstraint{
		Name:        name,
		TableName:   table,
		Type:        "CHECK",
		CheckClause: &expression,
	}
}

// constraintDatabase builds an introspected schema holding the two tables the
// fixtures name and the constraints given.
func constraintDatabase(constraints ...types.DBConstraint) func() *types.DBSchema {
	return func() *types.DBSchema {
		table := func(name string) types.DBTable {
			return types.DBTable{
				Name: name,
				Type: "TABLE",
				Columns: []types.DBColumn{
					{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
					{Name: "total", DataType: "integer", IsNullable: "NO"},
				},
			}
		}
		return &types.DBSchema{
			Tables:      []types.DBTable{table("orders"), table("invoices")},
			Constraints: slices.Clone(constraints),
		}
	}
}

func additionNames(infos []difftypes.ConstraintAdditionInfo) []string {
	names := make([]string, 0, len(infos))
	for _, info := range infos {
		names = append(names, info.Name)
	}
	return sortedCopy(names)
}

func removalNames(infos []difftypes.ConstraintRemovalInfo) []string {
	names := make([]string, 0, len(infos))
	for _, info := range infos {
		names = append(names, info.Name)
	}
	return sortedCopy(names)
}

// sortedCopy sorts a copy, so an assertion cannot reorder the diff it reads.
func sortedCopy(names []string) []string {
	if len(names) == 0 {
		return make([]string, 0)
	}
	return slices.Sorted(slices.Values(names))
}
