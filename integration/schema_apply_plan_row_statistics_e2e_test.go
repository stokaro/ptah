//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// A saved plan records the fingerprint of the state it was computed against,
// and schema apply --plan refuses it when the target no longer matches. Rows
// written to a table are not a schema change: they move the table's row
// estimate, and of that estimate a plan depends only on whether the table
// holds rows, which decides whether an index is built concurrently. So a plan
// survives rows written to a table that already held some, and goes stale once
// an empty table holds rows. ANALYZE makes the estimate the server reports
// settle before each read, rather than whenever its statistics are flushed.

// planRowStatisticsDesired adds a column to public.notes.
const planRowStatisticsDesired = "CREATE TABLE public.notes (id integer PRIMARY KEY, body text NOT NULL, title text);\n"

// planRowStatisticsSurface saves a plan with one binary and applies it.
type planRowStatisticsSurface struct {
	name   string
	target clirun.Target
	plan   func(database, schema, plan string) []string
	apply  func(database, schema, plan string) []string
}

var planRowStatisticsSurfaces = []planRowStatisticsSurface{
	{
		name:   "ptah",
		target: clirun.Ptah,
		plan: func(database, schema, plan string) []string {
			return []string{"schema", "plan", "--db-url", database, "--schema-file", schema, "--output", plan}
		},
		apply: func(database, _, plan string) []string {
			return []string{"schema", "apply", "--db-url", database, "--plan", plan, "--auto-approve"}
		},
	},
	{
		name:   "ptah-compat",
		target: clirun.Compat,
		plan: func(database, schema, plan string) []string {
			return []string{"schema", "plan", "--from", database, "--to", "file://" + filepath.ToSlash(schema), "--output", plan}
		},
		apply: func(database, schema, plan string) []string {
			return []string{"schema", "apply", "--url", database, "--to", "file://" + filepath.ToSlash(schema),
				"--plan", "file://" + filepath.ToSlash(plan), "--auto-approve"}
		},
	},
}

// planRowStatisticsCase is a table's rows when the plan is saved, and the rows
// written before it is applied.
type planRowStatisticsCase struct {
	name    string
	seed    string
	written string
	// want is the columns public.notes holds after the apply.
	want string
}

// runPlanRowStatistics saves the plan, writes the rows, applies the plan, and
// returns what the apply printed with the columns of public.notes.
func runPlanRowStatistics(c *qt.C, surface planRowStatisticsSurface, test planRowStatisticsCase) (clirun.Result, string) {
	c.Helper()
	database := newEmptyReadScopeDatabase(c, "plan_rows")
	execPostgresFamilySQL(c, c.Context(), database, "CREATE TABLE public.notes (id integer PRIMARY KEY, body text NOT NULL)")
	execPostgresFamilySQL(c, c.Context(), database, test.seed+"; ANALYZE public.notes")
	dir := c.TempDir()
	schema := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(planRowStatisticsDesired), 0o600), qt.IsNil)
	plan := filepath.Join(dir, "notes.plan.json")
	options := clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}

	planned := clirun.Run(c, surface.target, options, surface.plan(database, schema, plan)...)
	c.Assert(planned.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", planned.Stdout, planned.Stderr))
	execPostgresFamilySQL(c, c.Context(), database, test.written+"; ANALYZE public.notes")
	applied := clirun.Run(c, surface.target, options, surface.apply(database, schema, plan)...)
	return applied, notesColumns(c, database)
}

// notesColumns lists the columns of public.notes in order, comma-separated.
func notesColumns(c *qt.C, database string) string {
	c.Helper()
	var columns string
	err := openReadScopeDatabase(c, database).QueryRowContext(c.Context(), `
		SELECT string_agg(column_name, ',' ORDER BY ordinal_position)
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'notes'`).Scan(&columns)
	c.Assert(err, qt.IsNil)
	return columns
}

// TestSchemaApplyPlanOutlivesRowsWrittenToAPopulatedTable_HappyPathE2E saves a
// plan while public.notes holds one row, writes two more, and applies the
// plan. The estimate moves from one row to three, the table held rows
// throughout, and the plan applies.
func TestSchemaApplyPlanOutlivesRowsWrittenToAPopulatedTable_HappyPathE2E(t *testing.T) {
	test := planRowStatisticsCase{
		seed:    "INSERT INTO public.notes VALUES (1, 'a')",
		written: "INSERT INTO public.notes VALUES (2, 'b'), (3, 'c')",
		want:    "id,body,title",
	}
	for _, surface := range planRowStatisticsSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)

			applied, columns := runPlanRowStatistics(c, surface, test)

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(columns, qt.Equals, test.want)
		})
	}
}

// TestSchemaApplyPlanIsStaleOnceAnEmptyTableHoldsRows_FailurePathE2E is the
// control: the plan is saved while public.notes is empty, and a row is
// written before it is applied. Whether the table holds rows is part of what
// the plan was computed against, so the plan is refused as stale and the
// column is not added.
func TestSchemaApplyPlanIsStaleOnceAnEmptyTableHoldsRows_FailurePathE2E(t *testing.T) {
	test := planRowStatisticsCase{
		seed:    "SELECT 1",
		written: "INSERT INTO public.notes VALUES (1, 'a')",
		want:    "id,body",
	}
	for _, surface := range planRowStatisticsSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			c := qt.New(t)

			applied, columns := runPlanRowStatistics(c, surface, test)

			c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stderr, qt.Contains, "pre-planned migration is stale: the target database schema does not match "+
				"the plan's source fingerprint")
			c.Assert(columns, qt.Equals, test.want)
		})
	}
}
