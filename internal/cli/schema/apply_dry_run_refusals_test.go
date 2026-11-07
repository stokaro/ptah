package schema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/plangraph"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/cli/internal/exitcode"
)

// savedPlanRequiring saves the fixture's plan with an owner's transaction requirement
// on its statement, as a provider's plan records one. A hand edit is the way to hold
// one on a built-in engine, whose owners state none.
func savedPlanRequiring(c *qt.C, f planFixture, requirement plangraph.Transaction) string {
	c.Helper()
	planPath := f.savePlan(c)
	plan, err := atlasschema.ReadPlanFile(planPath)
	c.Assert(err, qt.IsNil)
	plan.Statements[0].Transactions = []plangraph.Transaction{requirement}
	contents, err := atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(planPath, contents, 0o600), qt.IsNil)
	return planPath
}

// TestSchemaApplyDryRunRefusesWhatARealRunRefuses runs one plan both ways, and the
// dry run has to give the real run's answer: the same error, exit code, outcome and
// refusal. A dry run that promised success would send the real run to its refusal.
func TestSchemaApplyDryRunRefusesWhatARealRunRefuses(t *testing.T) {
	tests := []struct {
		name        string
		requirement plangraph.Transaction
		mode        string
		wantError   string
	}{
		{
			name:        "a statement that needs a transaction, with none",
			requirement: plangraph.TransactionRequired,
			mode:        "none",
			wantError:   `.*planned statement 1 requires a transaction for its complete operation`,
		},
		{
			name:        "a statement that refuses a transaction, with one",
			requirement: plangraph.TransactionForbidden,
			mode:        "file",
			wantError:   `.*planned statement 1 .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newPlanFixture(c)
			planPath := savedPlanRequiring(c, f, test.requirement)
			args := []string{"apply", "--db-url", f.dbURL, "--plan", planPath, "--tx-mode", test.mode, "--json"}

			dryOut, _, dryErr := runSchemaStreams(append(args, "--dry-run")...)
			realOut, _, realErr := runSchemaStreams(append(args, "--auto-approve")...)

			c.Assert(dryErr, qt.ErrorMatches, test.wantError)
			c.Assert(dryErr.Error(), qt.Equals, realErr.Error())
			c.Assert(exitcode.Code(dryErr, 0), qt.Equals, exitcode.Code(realErr, 0))
			dry, ran := decodeOneDocument[atlasschema.ApplyReport](c, dryOut), decodeOneDocument[atlasschema.ApplyReport](c, realOut)
			c.Assert(dry, qt.DeepEquals, ran)
			c.Assert(dry.Outcome, qt.Equals, atlasschema.ApplyOutcomeRefused)
			c.Assert(listSQLiteTables(c, f.dbPath), qt.DeepEquals, []string{"users"})
		})
	}
}

// TestSchemaApplyDryRunReportsAFailedRehearsal holds --dry-run to the dev database's
// answer, on the plan [TestSchemaApplyJSONReportsASimulationFailure] rehearses.
func TestSchemaApplyDryRunReportsAFailedRehearsal(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	entities := filepath.Join(dir, "entities")
	c.Assert(os.MkdirAll(entities, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(entities, "schema.go"), []byte(countryCityEntities), 0o600), qt.IsNil)
	citiesFile := filepath.Join(entities, "cities.yaml")
	c.Assert(os.WriteFile(citiesFile, []byte("[]\n"), 0o600), qt.IsNil)
	dbPath := filepath.Join(dir, "target.db")
	dbURL := "sqlite://" + dbPath
	out, err := runSchema("", "apply", "--db-url", dbURL, "--root-dir", entities, "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	seedSQLite(c, dbPath, "INSERT INTO countries (code) VALUES ('NO');")
	c.Assert(os.WriteFile(citiesFile, []byte("- id: 1\n  country: NO\n"), 0o600), qt.IsNil)

	stdout, stderr, err := runSchemaStreams("apply", "--db-url", dbURL, "--root-dir", entities, "--dry-run", "--json",
		"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"))

	c.Assert(err, qt.ErrorMatches,
		`(?s)dev database simulation failed during plan: .*FOREIGN KEY constraint failed.*; the plan was not applied to the target database`)
	report := decodeOneDocument[atlasschema.ApplyReport](c, stdout)
	c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeRefused, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(report.Refusal, qt.DeepEquals, &atlasschema.Refusal{Code: atlasschema.RefusalSimulationFailed})
}

// TestSchemaApplyDryRunReachesThePlanItCanRun is the control. The checks a dry run
// now makes pass a plan that can run: it answers dry-run at exit 0, with a rehearsal
// that succeeded and nothing applied to the target.
func TestSchemaApplyDryRunReachesThePlanItCanRun(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)
	dev := "sqlite://" + filepath.Join(f.dir, "dev.db")

	stdout, stderr, err := runSchemaStreams("apply", "--db-url", f.dbURL, "--schema-file", f.schemaPath, "--dev-url", dev, "--dry-run", "--json")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	report := decodeOneDocument[atlasschema.ApplyReport](c, stdout)
	c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeDryRun)
	c.Assert(report.Refusal, qt.IsNil)
	c.Assert(listSQLiteTables(c, f.dbPath), qt.DeepEquals, []string{"users"})
}

// TestSchemaApplyDryRunAsksForNoApproval holds the exit ahead of the prompt: a dry run
// with no input answers, where a run asking for approval would fail to read it.
func TestSchemaApplyDryRunAsksForNoApproval(t *testing.T) {
	c := qt.New(t)
	f := newPlanFixture(c)

	stdout, _, err := runSchemaStreamsWithInput("", "apply", "--db-url", f.dbURL, "--schema-file", f.schemaPath, "--dry-run")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Not(qt.Contains), "Apply these schema changes?")
	c.Assert(listSQLiteTables(c, f.dbPath), qt.DeepEquals, []string{"users"})
}
