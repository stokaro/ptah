//go:build integration

package providercli_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
	"ptah.run/internal/testutils"
)

// A saved plan binds to the provider release, not to its executable. Another
// build of the release applies the plan, as reinstalling it or installing it
// on another platform does, and another release refuses it before writing.
func TestProviderNativePlanFileBindsTheProviderRelease(t *testing.T) {
	c := qt.New(t)
	planned := nativeCommandSpec(c, testutils.FixtureRelease{})
	rebuilt := nativeCommandSpec(c, testutils.FixtureRelease{Stripped: true})
	upgraded := nativeCommandSpec(c, testutils.FixtureRelease{Version: "0.0.2"})
	c.Assert(rebuilt.SHA256, qt.Not(qt.Equals), planned.SHA256)
	c.Assert(upgraded.SHA256, qt.Not(qt.Equals), planned.SHA256)

	t.Run("another build of the release applies the plan", func(t *testing.T) {
		c := qt.New(t)
		f := nativeCommandFixtureFor(c, planned)
		plan := savedNativePlan(c, f)
		c.Assert(plan.Provider.Version, qt.Equals, "0.0.1")
		c.Assert(plan.Statements[0].Native, qt.IsNotNil)
		f.selectProvider(c, rebuilt)
		applied := f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve", "--json")
		var report atlasschema.ApplyReport
		c.Assert(json.Unmarshal([]byte(applied), &report), qt.IsNil)
		c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeApplied)
		c.Assert(f.success(c, "inspect", "--format", "json"), qt.Contains, `"Name": "added"`)
	})

	t.Run("another release refuses the plan before writing", func(t *testing.T) {
		c := qt.New(t)
		f := nativeCommandFixtureFor(c, planned)
		savedNativePlan(c, f)
		f.selectProvider(c, upgraded)
		out, diagnostics, code := f.run(c, c.Context(), "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve", "--json")
		c.Assert(code, qt.Equals, 2)
		c.Assert(diagnostics, qt.Contains, `plan was computed with provider "example.org/fixture" version "0.0.1", `+
			`but the selected provider is version "0.0.2"; create and approve a new plan with the selected provider`)
		var report atlasschema.ApplyReport
		c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil)
		c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeFailed)
		c.Assert(f.success(c, "inspect", "--format", "json"), qt.Not(qt.Contains), `"Name": "added"`)
	})
}
