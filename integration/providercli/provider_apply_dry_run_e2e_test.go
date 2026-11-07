//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
	"ptah.run/internal/testutils"
)

// TestProviderSchemaApply_ADryRunAnswersAsARealRun runs one plan both ways on a
// provider that offers no transactions, in the mode that needs one. The dry run gives
// the real run's document and exit code, computed and saved alike. A dry run that
// promised success would send the real run to its refusal.
func TestProviderSchemaApply_ADryRunAnswersAsARealRun(t *testing.T) {
	spec := fixtureReleaseSpec(qt.New(t), "provider", testutils.FixtureRelease{})
	tests := []struct {
		name string
		args func(providerCLI, ...string) []string
	}{
		{name: "a computed plan", args: computedUnapprovedArgs},
		{name: "a saved plan", args: savedUnapprovedArgs},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newProviderCLI(c, spec)
			f.plan(c)

			dryOut, dryDiagnostics, dryCode := f.run(c, c.Context(), "apply", test.args(f, "--dry-run")...)
			realOut, realDiagnostics, realCode := f.run(c, c.Context(), "apply", test.args(f)...)

			c.Assert(dryCode, qt.Equals, 2, qt.Commentf("%s", dryDiagnostics))
			c.Assert(dryCode, qt.Equals, realCode, qt.Commentf("%s", realDiagnostics))
			report := applyReport(c, dryOut)
			c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeRefused)
			c.Assert(refusalCode(report), qt.Equals, atlasschema.RefusalTransactionUnavailable)
			c.Assert(dryDiagnostics, qt.Contains, noTransactionMessage)
			c.Assert(dryOut, qt.Equals, realOut)
			c.Assert(databaseTables(c, f.databasePath, "items", "second"), qt.HasLen, 0)
		})
	}
}

// TestProviderSchemaApply_ADryRunPassesThePlanARealRunApplies is the control: in the
// mode the provider can honor, the dry run writes nothing and the real run applies.
func TestProviderSchemaApply_ADryRunPassesThePlanARealRunApplies(t *testing.T) {
	spec := fixtureReleaseSpec(qt.New(t), "provider", testutils.FixtureRelease{})
	tests := []struct {
		name string
		args func(providerCLI, ...string) []string
	}{
		{name: "a computed plan", args: computedUnapprovedArgs},
		{name: "a saved plan", args: savedUnapprovedArgs},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := newProviderCLI(c, spec)
			f.plan(c)

			out, diagnostics, code := f.run(c, c.Context(), "apply", test.args(f, "--tx-mode", "none", "--dry-run")...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
			c.Assert(applyReport(c, out).Outcome, qt.Equals, atlasschema.ApplyOutcomeDryRun)
			c.Assert(databaseTables(c, f.databasePath, "items", "second"), qt.HasLen, 0)

			applied, diagnostics, code := f.run(c, c.Context(), "apply", test.args(f, "--tx-mode", "none", "--auto-approve")...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics))
			c.Assert(applyReport(c, applied).Outcome, qt.Equals, atlasschema.ApplyOutcomeApplied)
			c.Assert(databaseTables(c, f.databasePath, "items", "second"), qt.DeepEquals, []string{"items", "second"})
		})
	}
}

// TestProviderNativeRehearsalFailureRefusesADryRun_FailurePath holds a plan with a
// native operation to its rehearsal under --dry-run: the dev database's refusal is the
// answer a real run would get, and the target is left as it was.
func TestProviderNativeRehearsalFailureRefusesADryRun_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := newRehearsalCLI(c)

	_, diagnostics, code := f.run(c, c.Context(), "apply", "--schema-file", f.schemaPath, "--tx-mode", "none",
		"--dev-url", fixtureAddress("", f.devPath)+"?fault=reject", "--dry-run")

	c.Assert(code, qt.Equals, 2, qt.Commentf("%s", diagnostics))
	c.Assert(diagnostics, qt.Contains, "dev database simulation failed during baseline: ")
	c.Assert(f.columns(c), qt.DeepEquals, []string{"id"})
}

// TestProviderNativeRehearsalRunsUnderADryRun_HappyPath is its control: the rehearsal
// runs and passes, the target keeps its columns, and the dev database is handed back
// empty.
func TestProviderNativeRehearsalRunsUnderADryRun_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newRehearsalCLI(c)

	out := f.success(c, "apply", "--schema-file", f.schemaPath, "--tx-mode", "none", "--dev-url", fixtureAddress("", f.devPath),
		"--dry-run", "--json")

	c.Assert(applyReport(c, out).Outcome, qt.Equals, atlasschema.ApplyOutcomeDryRun)
	c.Assert(f.columns(c), qt.DeepEquals, []string{"id"})
	c.Assert(tables(c, f.devPath), qt.HasLen, 0)
}
