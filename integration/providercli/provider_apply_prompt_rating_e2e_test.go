//go:build integration

package providercli_test

import (
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
	"ptah.run/migration/safety"
)

// TestProviderNativeApplyPromptKeepsASafePlanQuiet is a plan that holds a native
// operation its owner rates safe, computed and saved. The prompt says nothing more,
// and a rating that failed would say so. It reads no input here, so the run fails to
// read an answer, as it does without a rating.
func TestProviderNativeApplyPromptKeepsASafePlanQuiet(t *testing.T) {
	tests := []struct {
		name string
		args func(providerCLI) []string
	}{
		{
			name: "a computed plan",
			args: func(f providerCLI) []string { return []string{"--schema-file", f.schemaPath, "--tx-mode", "none"} },
		},
		{
			name: "a saved plan",
			args: func(f providerCLI) []string { return []string{"--plan", f.planPath, "--tx-mode", "none"} },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			f := nativeCommandFixture(c)
			f.plan(c)

			out, diagnostics, code := f.run(c, c.Context(), "apply", test.args(f)...)

			c.Assert(code, qt.Equals, 2, qt.Commentf("%s", diagnostics))
			c.Assert(diagnostics, qt.Contains, "read schema apply confirmation: EOF")
			c.Assert(out, qt.Contains, "Apply these schema changes?")
			c.Assert(out, qt.Not(qt.Contains), "Plan rating")
			c.Assert(out, qt.Not(qt.Contains), "no rating")
		})
	}
}

// TestProviderNativeApplyPromptReadsARecordedVerdict rates a saved native operation by
// the verdict its plan records, since no text of it can say otherwise. The fixture's
// owner rates its one operation safe, so the file holds the raised verdict by hand.
func TestProviderNativeApplyPromptReadsARecordedVerdict(t *testing.T) {
	c := qt.New(t)
	f := nativeCommandFixture(c)
	f.plan(c)
	contents, err := os.ReadFile(f.planPath)
	c.Assert(err, qt.IsNil)
	plan, _, err := atlasschema.DecodePlanFile(contents, f.planPath)
	c.Assert(err, qt.IsNil)
	plan.Statements[0].Severity = safety.Destructive
	plan.Statements[0].Reason = "the owner says so"
	raised, err := atlasschema.MarshalPlanFile(plan)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(f.planPath, raised, 0o600), qt.IsNil) // #nosec G703 -- the path is under the test's own temporary directory.

	out, diagnostics, code := f.run(c, c.Context(), "apply", "--plan", f.planPath, "--tx-mode", "none")

	c.Assert(code, qt.Equals, 2, qt.Commentf("%s", diagnostics))
	c.Assert(out, qt.Contains, "Plan rating: destructive\nStatement 1 of 1, destructive: the owner says so\n  "+
		string(operationModel)+" items\nApply these schema changes?")
}
