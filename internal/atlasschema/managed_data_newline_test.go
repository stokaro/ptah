package atlasschema_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
	"ptah.run/migration/safety"
)

// A declared value may carry a newline. Inside a quoted literal it is ordinary
// SQL, so the rendered statement is one statement, and the plan has to carry it
// whole: a plan that reads the rendered script back line by line cuts the
// literal in two, files the tail by its own first word, and hands the engine a
// fragment that is not SQL (stokaro/ptah#3278).

// TestPreparePlanFile_ANewlineInADeclaredValueIsOneStatement plans a row into a
// table the same plan creates.
func TestPreparePlanFile_ANewlineInADeclaredValueIsOneStatement(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "newline-insert.db")

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		Desired: regionsSchema(regionRow("NL", "first line\nsecond line", 1)),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Statements, qt.HasLen, 2)
	c.Assert(
		plan.Statements[1].SQL,
		qt.Equals,
		"INSERT INTO \"regions\" (\"code\", \"name\", \"rank\") VALUES ('NL', 'first line\nsecond line', 1);",
	)
	c.Assert(plan.Statements[1].Severity, qt.Equals, safety.Safe)
}

// TestPreparePlanFile_ANewlineInAnUpdatedValueIsOneWarning is the value reaching
// an existing row. Cut at the newline, the tail of the UPDATE would read as a
// safe INSERT and run in the insert phase, ahead of its own head.
func TestPreparePlanFile_ANewlineInAnUpdatedValueIsOneWarning(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "newline-update.db")
	applyPlan(c, conn, regionsSchema(regionRow("NO", "Norway", 1)))

	desired := regionsSchema(regionRow("NO", "Nor\nway", 1))
	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{Desired: desired})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Statements, qt.HasLen, 1)
	c.Assert(plan.Statements[0].SQL, qt.Equals, "UPDATE \"regions\" SET \"name\" = 'Nor\nway', \"rank\" = 1 WHERE \"code\" = 'NO';")
	c.Assert(plan.Statements[0].Severity, qt.Equals, safety.Warning)

	applyPlan(c, conn, desired)
	var name string
	row := conn.QueryRowContext(context.Background(), `SELECT name FROM regions WHERE code = 'NO'`)
	c.Assert(row.Scan(&name), qt.IsNil)
	c.Assert(name, qt.Equals, "Nor\nway")
}

// TestPreparePlanFile_ANewlineInADeletedKeyIsDestructive is the removal of a row
// whose key carries a newline. The row is written through the driver, so the
// plan under test is the DELETE alone.
func TestPreparePlanFile_ANewlineInADeletedKeyIsDestructive(t *testing.T) {
	c := qt.New(t)
	conn := managedDataConnection(c, "newline-delete.db")
	applyPlan(c, conn, regionsSchema())
	_, err := conn.ExecContext(
		context.Background(),
		`INSERT INTO regions (code, name, rank) VALUES (?, ?, ?)`,
		"first\nsecond", "Gone", 1,
	)
	c.Assert(err, qt.IsNil)

	plan, err := atlasschema.PreparePlanFile(context.Background(), conn, atlasschema.PlanFileOptions{
		Desired: regionsSchema(),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Statements, qt.HasLen, 1)
	c.Assert(plan.Statements[0].SQL, qt.Equals, "DELETE FROM \"regions\" WHERE \"code\" = 'first\nsecond';")
	c.Assert(plan.Statements[0].Severity, qt.Equals, safety.Destructive)
	c.Assert(plan.Destructive, qt.IsTrue)
}
