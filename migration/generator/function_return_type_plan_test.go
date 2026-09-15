package generator_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/generator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// PostgreSQL refuses CREATE OR REPLACE for a routine whose return type changes
// (SQLSTATE 42P13), so both directions of such a change drop the routine and
// create it again (stokaro/ptah#3288). Each drop has to address the routine the
// database holds when that direction runs, which for the rollback is the
// routine the forward direction created, and each create has to restore the
// overload the change was made to.

// planRoutineChange compares declared routines with recorded routines and
// renders both directions of the plan.
func planRoutineChange(
	c *qt.C,
	desired []schemamodel.Function,
	current []catalog.Function,
) (forward, reverse string) {
	c.Helper()
	wanted := &schemamodel.Database{Functions: desired}
	live := &catalog.Database{Functions: current}
	diff := schemadiff.CompareWithDialect(wanted, live, platform.Postgres)
	c.Assert(diff.FunctionsModified, qt.HasLen, 1)

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: wanted,
		CurrentSchema: live,
		Dialect:       platform.Postgres,
	})
	c.Assert(err, qt.IsNil)

	up, err := planner.GenerateSchemaDiffSQLStatements(plan.Forward.Diff, platform.Postgres)
	c.Assert(err, qt.IsNil)
	down, err := planner.GenerateSchemaDiffSQLStatements(plan.Reverse.Diff, platform.Postgres)
	c.Assert(err, qt.IsNil)
	return strings.Join(up, "\n"), strings.Join(down, "\n")
}

// recordedTotal is a routine as the PostgreSQL reader reports it.
func recordedTotal(parameters, returns string) catalog.Function {
	return catalog.Function{
		Name:              "total",
		Parameters:        parameters,
		IdentityArguments: new(parameters),
		Returns:           returns,
		Language:          "sql",
		Security:          "INVOKER",
		Volatility:        "VOLATILE",
		Body:              "SELECT 1",
	}
}

// declaredTotal is a routine as a schema declares it.
func declaredTotal(parameters, returns string) schemamodel.Function {
	return schemamodel.Function{
		Name:       "total",
		Parameters: parameters,
		Returns:    returns,
		Language:   "sql",
		Security:   "INVOKER",
		Volatility: "VOLATILE",
		Body:       "SELECT 1",
	}
}

func TestPlanBidirectionalSchemaDiff_ReturnTypeChangeRebuildsInBothDirections(t *testing.T) {
	c := qt.New(t)

	forward, reverse := planRoutineChange(c,
		[]schemamodel.Function{declaredTotal("n integer", "bigint")},
		[]catalog.Function{recordedTotal("n integer", "integer")},
	)

	c.Assert(forward, qt.Matches,
		`(?s).*DROP FUNCTION "total"\(n integer\).*CREATE OR REPLACE FUNCTION "total"\(n integer\) RETURNS bigint.*`)
	c.Assert(reverse, qt.Matches,
		`(?s).*DROP FUNCTION "total"\(n integer\).*CREATE OR REPLACE FUNCTION "total"\(n integer\) RETURNS integer.*`)
}

// When the arguments change with the return type, the rollback drops the
// overload the forward direction created, which only the declaration names.
func TestPlanBidirectionalSchemaDiff_ReturnAndArgumentChangeRollbackDropsTheCreatedRoutine(t *testing.T) {
	c := qt.New(t)

	forward, reverse := planRoutineChange(c,
		[]schemamodel.Function{declaredTotal("m bigint", "bigint")},
		[]catalog.Function{recordedTotal("n integer", "integer")},
	)

	c.Assert(forward, qt.Matches,
		`(?s).*DROP FUNCTION "total"\(n integer\).*CREATE OR REPLACE FUNCTION "total"\(m bigint\) RETURNS bigint.*`)
	c.Assert(reverse, qt.Matches,
		`(?s).*DROP FUNCTION "total"\(m bigint\).*CREATE OR REPLACE FUNCTION "total"\(n integer\) RETURNS integer.*`)
}

// The rollback restores the overload that changed. The name alone selects the
// first routine that carries it, which here is the zero-argument overload.
func TestPlanBidirectionalSchemaDiff_OverloadRollbackRestoresTheChangedOverload(t *testing.T) {
	c := qt.New(t)

	_, reverse := planRoutineChange(c,
		[]schemamodel.Function{declaredTotal("", "integer"), declaredTotal("n integer", "bigint")},
		[]catalog.Function{recordedTotal("", "integer"), recordedTotal("n integer", "integer")},
	)

	c.Assert(reverse, qt.Matches,
		`(?s).*DROP FUNCTION "total"\(n integer\).*CREATE OR REPLACE FUNCTION "total"\(n integer\) RETURNS integer.*`)
	c.Assert(reverse, qt.Not(qt.Contains), `CREATE OR REPLACE FUNCTION "total"() RETURNS integer`)
}
