//go:build integration

package providercli_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasschema"
)

func savedNativePlan(c *qt.C, f providerCLI) atlasschema.PlanFile {
	c.Helper()
	saved := f.success(c, "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--json")
	var report atlasschema.PlanReport
	c.Assert(json.Unmarshal([]byte(saved), &report), qt.IsNil)
	c.Assert(report.Outcome, qt.Equals, atlasschema.PlanOutcomeChanges)
	c.Assert(report.Plan, qt.IsNotNil)
	return *report.Plan
}

func TestProviderNativePlanFileRecordsOperations(t *testing.T) {
	c := qt.New(t)
	f := nativeCommandFixture(c)
	plan := savedNativePlan(c, f)
	c.Assert(plan.Statements, qt.HasLen, 1)
	c.Assert(plan.Statements[0].SQL, qt.Equals, "")
	c.Assert(plan.Statements[0].Native, qt.IsNotNil)
	c.Assert(plan.Statements[0].Native.Kind, qt.Equals, operationModel)
	c.Assert(plan.Statements[0].Native.Subject, qt.Equals, "items")
	c.Assert(plan.Provider, qt.IsNotNil)
	document, err := os.ReadFile(f.planPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(document), qt.Contains, `"native"`)
	c.Assert(f.success(c, "inspect", "--format", "json"), qt.Not(qt.Contains), `"Name": "added"`)
}

func TestProviderNativePlanFileApplies(t *testing.T) {
	c := qt.New(t)
	f := nativeCommandFixture(c)
	savedNativePlan(c, f)
	applied := f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--auto-approve", "--json")
	var report atlasschema.ApplyReport
	c.Assert(json.Unmarshal([]byte(applied), &report), qt.IsNil)
	c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeApplied)
	c.Assert(report.Units, qt.HasLen, 1)
	c.Assert(report.Units[0].Native, qt.IsNotNil)
	c.Assert(f.success(c, "inspect", "--format", "json"), qt.Contains, `"Name": "added"`)
	c.Assert(f.success(c, "plan", "--schema-file", f.schemaPath, "--output", f.planPath, "--json"), qt.Contains, `"outcome": "no-changes"`)
}

func TestProviderNativePlanFileDryRunWritesNothing(t *testing.T) {
	c := qt.New(t)
	f := nativeCommandFixture(c)
	savedNativePlan(c, f)
	previewed := f.success(c, "apply", "--plan", f.planPath, "--tx-mode", "none", "--dry-run", "--json")
	var report atlasschema.ApplyReport
	c.Assert(json.Unmarshal([]byte(previewed), &report), qt.IsNil)
	c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeDryRun)
	c.Assert(f.success(c, "inspect", "--format", "json"), qt.Not(qt.Contains), `"Name": "added"`)
}

func TestProviderNativePlanFileRefusesBeforeWriting(t *testing.T) {
	c := qt.New(t)
	f := nativeCommandFixture(c)
	savedNativePlan(c, f)
	document, err := os.ReadFile(f.planPath)
	c.Assert(err, qt.IsNil)
	definition := regexp.MustCompile(`("definition": ")sha256:[0-9a-f]{64}`)
	c.Assert(definition.Match(document), qt.IsTrue)
	tampered := filepath.Join(c.TempDir(), "tampered.plan.json")
	changed := definition.ReplaceAll(document, []byte("${1}sha256:"+strings.Repeat("0", 64)))
	c.Assert(os.WriteFile(tampered, changed, 0o600), qt.IsNil) // #nosec G703 -- the path is c.TempDir() plus a constant name
	for _, test := range []struct {
		name string
		plan string
		args []string
		want string
	}{
		{"conflicting transaction mode", f.planPath, []string{"--tx-mode", "file"}, "requires execution outside a transaction"},
		{"changed operation definition", tampered, []string{"--tx-mode", "none"}, "decode native operations"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			args := append([]string{"--plan", test.plan, "--auto-approve", "--json"}, test.args...)
			_, diagnostics, code := f.run(c, c.Context(), "apply", args...)
			c.Assert(code, qt.Not(qt.Equals), 0)
			c.Assert(diagnostics, qt.Contains, test.want)
			c.Assert(f.success(c, "inspect", "--format", "json"), qt.Not(qt.Contains), `"Name": "added"`)
		})
	}
}
