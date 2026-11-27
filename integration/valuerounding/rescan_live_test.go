//go:build integration

package valuerounding_test

import (
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/planrefusal"
	"ptah.run/migration/migrator"
)

// lowerScale declares prices.amount with one digit after the point.
func lowerScale() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Price", Name: "prices"}},
		Fields: []schemamodel.Field{
			{StructName: "Price", Name: "id", Type: "int", Primary: true},
			{StructName: "Price", Name: "amount", Type: "numeric(8,1)", Nullable: true},
		},
	}
}

// TestApplyPlanReadsTheValuesAgainBeforeItRunsLive plans the lower scale over a
// row that fits, and inserts a row with digits between the planning and the
// execution, where an apply waits for its approval. The execution reads the rows
// again before its first statement, refuses, and the value stays.
func TestApplyPlanReadsTheValuesAgainBeforeItRunsLive(t *testing.T) {
	c := qt.New(t)
	target := postgresServer.scratch(c, "CREATE TABLE prices (id int PRIMARY KEY, amount numeric(8,3))",
		"INSERT INTO prices VALUES (4, 1.5)")
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	plan, err := atlasschema.PlanApply(c.Context(), conn, atlasschema.ApplyOptions{Desired: lowerScale(), Runtime: must.Must(builtin.New())})
	c.Assert(err, qt.IsNil)
	run(c, target, "INSERT INTO prices VALUES (2, 1234.543)")

	err = plan.Execute(c.Context(), conn, migrator.MigrationTxModeFile)

	c.Assert(err, qt.ErrorAs, new(*planrefusal.RoundedValuesError))
	c.Assert(err, qt.ErrorMatches, `(?s)refusing a type change .*column "amount" of table "public.prices" from numeric\(8,3\) to numeric\(8,1\), where 1 row holds a value .*a row written since holds one`)
	c.Assert(read(c, target, "SELECT string_agg(amount::text, ',' ORDER BY id) FROM prices"), qt.Equals, "1234.543,1.500")
}

// TestApplyPlanRunsWhenNoRowCameToHoldDigitsLive is the control: with no row
// written between the planning and the execution, the same plan runs.
func TestApplyPlanRunsWhenNoRowCameToHoldDigitsLive(t *testing.T) {
	c := qt.New(t)
	target := postgresServer.scratch(c, "CREATE TABLE prices (id int PRIMARY KEY, amount numeric(8,3))",
		"INSERT INTO prices VALUES (4, 1.5)")
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	plan, err := atlasschema.PlanApply(c.Context(), conn, atlasschema.ApplyOptions{Desired: lowerScale(), Runtime: must.Must(builtin.New())})
	c.Assert(err, qt.IsNil)
	run(c, target, "INSERT INTO prices VALUES (2, 1234.500)")

	err = plan.Execute(c.Context(), conn, migrator.MigrationTxModeFile)

	c.Assert(err, qt.IsNil)
	c.Assert(read(c, target, "SELECT string_agg(amount::text, ',' ORDER BY id) FROM prices"), qt.Equals, "1234.5,1.5")
}

// TestSavedPlanReadsTheValuesAgainBeforeItRunsLive saves the lower scale over a
// row that fits, and inserts a row with digits before the saved plan runs. Its
// execution reads the rows again before the first statement, for a caller that
// did not check the target right before it too, refuses, and the value stays.
func TestSavedPlanReadsTheValuesAgainBeforeItRunsLive(t *testing.T) {
	c := qt.New(t)
	target := postgresServer.scratch(c, "CREATE TABLE prices (id int PRIMARY KEY, amount numeric(8,3))",
		"INSERT INTO prices VALUES (4, 1.5)")
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	plan, err := atlasschema.PreparePlanFile(c.Context(), conn, atlasschema.PlanFileOptions{
		Desired: lowerScale(), Runtime: must.Must(builtin.New()),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Roundings, qt.HasLen, 1)
	run(c, target, "INSERT INTO prices VALUES (2, 1234.543)")

	err = plan.Execute(c.Context(), conn, migrator.MigrationTxModeFile)

	c.Assert(err, qt.ErrorAs, new(*planrefusal.RoundedValuesError))
	c.Assert(err, qt.ErrorMatches, `(?s)refusing a type change .*column "amount" of table "public.prices" from numeric\(8,3\) to numeric\(8,1\), where 1 row holds a value .*The plan found no such value when it read the table.*`)
	c.Assert(read(c, target, "SELECT string_agg(amount::text, ',' ORDER BY id) FROM prices"), qt.Equals, "1234.543,1.500")
}

// TestSavedPlanRunsWhenNoRowCameToHoldDigitsLive is the control: a row written
// before the saved plan runs whose digits the new scale keeps lets it run.
func TestSavedPlanRunsWhenNoRowCameToHoldDigitsLive(t *testing.T) {
	c := qt.New(t)
	target := postgresServer.scratch(c, "CREATE TABLE prices (id int PRIMARY KEY, amount numeric(8,3))",
		"INSERT INTO prices VALUES (4, 1.5)")
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	plan, err := atlasschema.PreparePlanFile(c.Context(), conn, atlasschema.PlanFileOptions{
		Desired: lowerScale(), Runtime: must.Must(builtin.New()),
	})
	c.Assert(err, qt.IsNil)
	run(c, target, "INSERT INTO prices VALUES (2, 1234.500)")

	err = plan.Execute(c.Context(), conn, migrator.MigrationTxModeFile)

	c.Assert(err, qt.IsNil)
	c.Assert(read(c, target, "SELECT string_agg(amount::text, ',' ORDER BY id) FROM prices"), qt.Equals, "1234.5,1.5")
}

// A caller can hold and edit a plan without decoding it from a file. Both
// execution entry points must compare the recorded type with the ALTER before
// a scan can incorrectly report that the newly written row fits.
func TestSavedPlanRefusesAnEditedRoundingTypeLive(t *testing.T) {
	c := qt.New(t)
	target := postgresServer.scratch(c, "CREATE TABLE prices (id int PRIMARY KEY, amount numeric(8,3))",
		"INSERT INTO prices VALUES (4, 1.5)")
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	runtime := must.Must(builtin.New())
	plan, err := atlasschema.PreparePlanFile(c.Context(), conn, atlasschema.PlanFileOptions{
		Desired: lowerScale(), Runtime: runtime,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Roundings, qt.HasLen, 1)
	plan.Roundings[0].DeclaredType = "numeric(8,2)"
	run(c, target, "INSERT INTO prices VALUES (2, 1234.550)")

	verifyErr := atlasschema.VerifyPlanTarget(c.Context(), conn, plan, runtime)
	executeErr := plan.Execute(c.Context(), conn, migrator.MigrationTxModeFile)
	execution, captureErr := plan.ExecutionPlan(c.Context(), conn, runtime)

	c.Assert(verifyErr, qt.ErrorMatches, `plan rounding 1 is not a change the plan records: .*declared type .* differs from the statement's type .*`)
	c.Assert(executeErr, qt.ErrorMatches, `plan rounding 1 is not a change the plan records: .*declared type .* differs from the statement's type .*`)
	c.Assert(captureErr, qt.ErrorMatches, `plan rounding 1 is not a change the plan records: .*declared type .* differs from the statement's type .*`)
	c.Assert(execution.HasChanges(), qt.IsFalse)
	c.Assert(read(c, target, "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = 'prices'::regclass AND attname = 'amount'"), qt.Equals, "numeric(8,3)")
	c.Assert(read(c, target, "SELECT string_agg(amount::text, ',' ORDER BY id) FROM prices"), qt.Equals, "1234.550,1.500")
}

// CockroachDB compares bare INT as the width its session chooses, but renders
// INT itself. A generated file must scan that rendered spelling and survive
// decoding and execution without treating int8 as a globally equivalent alias.
func TestSavedPlanKeepsTheRenderedIntegerTypeCockroachLive(t *testing.T) {
	c := qt.New(t)
	engine := engineNamed(c, "cockroachdb")
	target := engine.server.scratch(c, "CREATE TABLE prices (id int PRIMARY KEY, amount numeric(8,3))",
		"INSERT INTO prices VALUES (4, 1.000)")
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	desired := lowerScale()
	desired.Fields[1].Type = "INT"
	plan, err := atlasschema.PreparePlanFile(c.Context(), conn, atlasschema.PlanFileOptions{
		Desired: desired, Runtime: must.Must(builtin.New()),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Roundings, qt.HasLen, 1)
	c.Assert(plan.Roundings[0].DeclaredType, qt.Equals, "INT")
	contents, err := json.Marshal(plan)
	c.Assert(err, qt.IsNil)

	decoded, _, err := atlasschema.DecodePlanFile(contents, "integer.plan.json")
	c.Assert(err, qt.IsNil)
	err = decoded.Execute(c.Context(), conn, migrator.MigrationTxModeFile)

	c.Assert(err, qt.IsNil)
	c.Assert(read(c, target, "SELECT amount::text FROM prices"), qt.Equals, "1")
}

// editedSQL is the SQL of plan with its type spelled in capitals: an edit of the
// reviewed text that changes no statement's meaning.
func editedSQL(c *qt.C, plan atlasschema.ApplyRuntimePlan) string {
	c.Helper()
	original, err := plan.SQL()
	c.Assert(err, qt.IsNil)
	edited := strings.Replace(original, "numeric(8,1)", "NUMERIC(8,1)", 1)
	c.Assert(edited, qt.Not(qt.Equals), original)
	return edited
}

// TestEditedApplyReadsTheValuesAgainBeforeItRunsLive plans the lower scale over
// a row that fits, edits the SQL as a reviewer can before approving it, and
// inserts a row with digits before the edited SQL runs. The edited SQL runs
// apart from the plan's own statements, and it reads the rows again before it
// runs too, refuses, and the value stays.
func TestEditedApplyReadsTheValuesAgainBeforeItRunsLive(t *testing.T) {
	c := qt.New(t)
	target := postgresServer.scratch(c, "CREATE TABLE prices (id int PRIMARY KEY, amount numeric(8,3))",
		"INSERT INTO prices VALUES (4, 1.5)")
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	plan, err := atlasschema.PrepareApply(c.Context(), conn, atlasschema.ApplyRuntimeOptions{
		Desired: lowerScale(), Runtime: must.Must(builtin.New()), TxMode: migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)
	edited := editedSQL(c, plan)
	run(c, target, "INSERT INTO prices VALUES (2, 1234.543)")

	err = plan.ExecuteSQL(c.Context(), edited)

	c.Assert(err, qt.ErrorAs, new(*planrefusal.RoundedValuesError))
	c.Assert(err, qt.ErrorMatches, `(?s)refusing a type change .*column "amount" of table "public.prices" from numeric\(8,3\) to numeric\(8,1\), where 1 row holds a value .*a row written since holds one`)
	c.Assert(read(c, target, "SELECT string_agg(amount::text, ',' ORDER BY id) FROM prices"), qt.Equals, "1234.543,1.500")
}

// TestEditedApplyRunsWhenNoRowCameToHoldDigitsLive is the control: the edited
// SQL runs when no row written since holds digits the new scale has no room for.
func TestEditedApplyRunsWhenNoRowCameToHoldDigitsLive(t *testing.T) {
	c := qt.New(t)
	target := postgresServer.scratch(c, "CREATE TABLE prices (id int PRIMARY KEY, amount numeric(8,3))",
		"INSERT INTO prices VALUES (4, 1.5)")
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	plan, err := atlasschema.PrepareApply(c.Context(), conn, atlasschema.ApplyRuntimeOptions{
		Desired: lowerScale(), Runtime: must.Must(builtin.New()), TxMode: migrator.MigrationTxModeFile,
	})
	c.Assert(err, qt.IsNil)
	edited := editedSQL(c, plan)
	run(c, target, "INSERT INTO prices VALUES (2, 1234.500)")

	err = plan.ExecuteSQL(c.Context(), edited)

	c.Assert(err, qt.IsNil)
	c.Assert(read(c, target, "SELECT string_agg(amount::text, ',' ORDER BY id) FROM prices"), qt.Equals, "1234.5,1.5")
}
