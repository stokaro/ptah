//go:build integration

package providercli_test

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/renderer"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/testutils"
	"ptah.run/migration/migrator"
	"ptah.run/migration/safety"
	"ptah.run/provider-sdk/provider"
)

func nativeCommandFixture(c *qt.C) providerCLI {
	c.Helper()
	return nativeCommandFixtureFor(c, nativeCommandSpec(c, testutils.FixtureRelease{}))
}

// nativeCommandSpec builds the operation-models fixture as release selects, in
// the mode that plans a new column as one native operation.
func nativeCommandSpec(c *qt.C, release testutils.FixtureRelease) providerhost.Spec {
	c.Helper()
	spec := fixtureReleaseSpec(c, "operation-models", release)
	spec.Environment = []string{"FIXTURE_RENDER_MODE=native-cli"}
	return spec
}

// nativeCommandFixtureFor creates the table through spec and leaves a desired
// schema that adds one column to it.
func nativeCommandFixtureFor(c *qt.C, spec providerhost.Spec) providerCLI {
	c.Helper()
	f := newProviderCLI(c, spec)
	c.Assert(os.WriteFile(f.schemaPath, []byte("tables:\n  items:\n    columns:\n      id: {type: INTEGER, not_null: true}\n"), 0o600), qt.IsNil)
	f.success(c, "apply", "--schema-file", f.schemaPath, "--tx-mode", "none", "--auto-approve")
	c.Assert(os.WriteFile(f.schemaPath, []byte("tables:\n  items:\n    columns:\n      id: {type: INTEGER, not_null: true}\n      added: {type: REAL}\n"), 0o600), qt.IsNil)
	return f
}

// TestProviderNativeCommands_RefuseAProviderThatCannotPlanTheStatementStyle is
// the cost of one statement style for every Ptah command. A provider that plans
// and does not list planning.statement-style is refused before anything runs,
// and the refusal names the provider and the feature. Sending the plan anyway
// would let the provider write its own guards and CASCADE where the command
// promises neither.
func TestProviderNativeCommands_RefuseAProviderThatCannotPlanTheStatementStyle(t *testing.T) {
	tests := []struct {
		verb string
		args func(f providerCLI) []string
	}{
		{verb: "apply", args: func(f providerCLI) []string {
			return []string{"--schema-file", f.schemaPath, "--tx-mode", "none", "--auto-approve"}
		}},
		{verb: "plan", args: func(f providerCLI) []string {
			return []string{"--schema-file", f.schemaPath, "--output", f.planPath}
		}},
	}
	for _, test := range tests {
		t.Run(test.verb, func(t *testing.T) {
			c := qt.New(t)
			spec := fixtureReleaseSpec(c, "operation-models", testutils.FixtureRelease{})
			spec.Environment = []string{"FIXTURE_RENDER_MODE=native-cli-unstyled"}
			f := newProviderCLI(c, spec)
			c.Assert(os.WriteFile(f.schemaPath, []byte("tables:\n  items:\n    columns:\n      id: {type: INTEGER, not_null: true}\n"), 0o600), qt.IsNil)

			_, diagnostics, code := f.run(c, c.Context(), test.verb, test.args(f)...)

			c.Assert(code, qt.Equals, 2, qt.Commentf("%s", diagnostics))
			c.Assert(diagnostics, qt.Contains, "does not list "+provider.PlanningStatementStyleFeature+"; upgrade the provider")
		})
	}
}

func TestProviderNativeSchemaApplyCommand(t *testing.T) {
	c := qt.New(t)
	f := nativeCommandFixture(c)
	applied := f.success(c, "apply", "--schema-file", f.schemaPath, "--tx-mode", "none", "--auto-approve", "--json")
	var report atlasschema.ApplyReport
	c.Assert(json.Unmarshal([]byte(applied), &report), qt.IsNil)
	c.Assert(report.ContractVersion, qt.Equals, atlasschema.ApplyReportContractVersion)
	c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeApplied)
	c.Assert(report.Units, qt.HasLen, 1)
	c.Assert(report.Units[0].SQL, qt.Equals, "")
	c.Assert(report.Units[0].Native, qt.IsNotNil)
	c.Assert(report.Units[0].Native.Kind, qt.Equals, operationModel)
	c.Assert(report.Units[0].Native.Subject, qt.Equals, "items")
	c.Assert(report.Units[0].Native.Severity, qt.Equals, safety.Safe)
	c.Assert(f.success(c, "inspect", "--format", "json"), qt.Contains, `"Name": "added"`)
	c.Assert(f.success(c, "apply", "--schema-file", f.schemaPath, "--tx-mode", "none", "--auto-approve", "--json"), qt.Contains, `"outcome": "no-changes"`)
}

func TestProviderNativeSchemaApplyRefusesTransactionBeforeWrite(t *testing.T) {
	c := qt.New(t)
	f := nativeCommandFixture(c)
	_, diagnostics, code := f.run(c, c.Context(), "apply", "--schema-file", f.schemaPath, "--tx-mode", "file", "--auto-approve", "--json")
	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, "requires execution outside a transaction")
	c.Assert(f.success(c, "inspect", "--format", "json"), qt.Not(qt.Contains), `"Name": "added"`)
}

func TestProviderNativePreparedPlanPreservesOperations(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native", "FIXTURE_STATEMENT_STYLE=honored"})
	conn := observedConnection(c, host, "planned")
	plan, err := atlasschema.PrepareApply(c.Context(), conn, atlasschema.ApplyRuntimeOptions{Runtime: runtime, Desired: desiredProviderColumn(), TxMode: migrator.MigrationTxModeNone})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.HasNativeUnits(), qt.IsTrue)
	units, err := plan.Units()
	c.Assert(err, qt.IsNil)
	c.Assert(units, qt.HasLen, 1)
	assessments, err := plan.Assessments(c.Context(), runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(assessments, qt.HasLen, 1)
	c.Assert(assessments[0].NativeKind, qt.Equals, operationModel)
	c.Assert(assessments[0].Subject, qt.Equals, "planned")
	c.Assert(plan.Execute(c.Context()), qt.IsNil)
	observed, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(observed.Tables[0].Columns, qt.HasLen, 2)
	c.Assert(observed.Tables[0].Columns[1].Name, qt.Equals, "added")
}

func TestProviderNativeSchemaApplyPreviewWritesNothing(t *testing.T) {
	c := qt.New(t)
	f := nativeCommandFixture(c)
	previewed := f.success(c, "apply", "--schema-file", f.schemaPath, "--tx-mode", "none", "--dry-run", "--json")
	var report atlasschema.ApplyReport
	c.Assert(json.Unmarshal([]byte(previewed), &report), qt.IsNil)
	c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeDryRun)
	c.Assert(report.Units, qt.HasLen, 1)
	c.Assert(report.Units[0].Native, qt.IsNotNil)
	c.Assert(report.Units[0].Native.Subject, qt.Equals, "items")
	c.Assert(f.success(c, "inspect", "--format", "json"), qt.Not(qt.Contains), `"Name": "added"`)
}

func TestProviderNativeSchemaApplyCancellation(t *testing.T) {
	c := qt.New(t)
	f := nativeCommandFixture(c)
	command := root.NewRootCommand()
	var out, diagnostics bytes.Buffer
	command.SetOut(&out)
	command.SetErr(&diagnostics)
	command.SetIn(strings.NewReader("NO\n"))
	code := root.RunContext(c.Context(), command, f.arguments("apply", "--schema-file", f.schemaPath, "--tx-mode", "none", "--json")...)
	f.assertStopped(c)
	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", diagnostics.String()))
	var report atlasschema.ApplyReport
	c.Assert(json.Unmarshal(out.Bytes(), &report), qt.IsNil)
	c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeCanceled)
	c.Assert(report.Units, qt.HasLen, 1)
	c.Assert(report.Units[0].Native, qt.IsNotNil)
	c.Assert(report.Units[0].Native.Subject, qt.Equals, "items")
	c.Assert(diagnostics.String(), qt.Contains, "Native operation:")
	c.Assert(f.success(c, "inspect", "--format", "json"), qt.Not(qt.Contains), `"Name": "added"`)
}

func TestProviderNativeSchemaApplyRefusesSQLEditing(t *testing.T) {
	c := qt.New(t)
	f := nativeCommandFixture(c)
	_, diagnostics, code := f.run(c, c.Context(), "apply", "--schema-file", f.schemaPath, "--tx-mode", "none", "--edit")
	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, "a native operation cannot be edited as SQL")
	c.Assert(f.success(c, "inspect", "--format", "json"), qt.Not(qt.Contains), `"Name": "added"`)
}

func TestProviderNativePreparedDryRunDoesNotWrite(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native", "FIXTURE_STATEMENT_STYLE=honored"})
	conn := observedConnection(c, host, "planned")
	plan, err := atlasschema.PrepareApply(c.Context(), conn, atlasschema.ApplyRuntimeOptions{Runtime: runtime, Desired: desiredProviderColumn(), TxMode: migrator.MigrationTxModeNone, DryRun: true})
	c.Assert(err, qt.IsNil)
	c.Assert(plan.HasNativeUnits(), qt.IsTrue)
	c.Assert(plan.Execute(c.Context()), qt.IsNil)
	observed, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(observed.Tables[0].Columns, qt.HasLen, 1)
}

func TestProviderNativePreparedPlanRefusesSQLProjection(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native", "FIXTURE_STATEMENT_STYLE=honored"})
	conn := observedConnection(c, host, "planned")
	plan, err := atlasschema.PrepareApply(c.Context(), conn, atlasschema.ApplyRuntimeOptions{Runtime: runtime, Desired: desiredProviderColumn(), TxMode: migrator.MigrationTxModeNone})
	c.Assert(err, qt.IsNil)
	sql, err := plan.SQL()
	c.Assert(err, qt.ErrorIs, renderer.ErrNativeSQL)
	c.Assert(sql, qt.Equals, "")
	statements, err := plan.Statements()
	c.Assert(err, qt.ErrorIs, renderer.ErrNativeSQL)
	c.Assert(statements, qt.IsNil)
}

// A rehearsal of a native plan refuses before it connects to anything when
// the dev URL names no engine the run serves, or when edited SQL is offered in
// place of the plan's operations.
func TestProviderNativePreparedPlanRefusesARehearsalBeforeConnect_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name    string
		opts    atlasschema.SimulateOptions
		wantErr string
	}{
		{"a dev URL of no engine", atlasschema.SimulateOptions{DevURL: "unsupported://must-not-connect"},
			`unsupported --dev-url dialect "unsupported://must-not-connect"`},
		{"edited SQL", atlasschema.SimulateOptions{DevURL: "unsupported://must-not-connect", Statements: []string{`ALTER TABLE "planned" ADD COLUMN "added" REAL;`}},
			`unsupported feature: edited SQL cannot stand in for a plan that holds native operations or approved inputs`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			host, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native", "FIXTURE_STATEMENT_STYLE=honored"})
			conn := observedConnection(c, host, "planned")
			plan, err := atlasschema.PrepareApply(c.Context(), conn, atlasschema.ApplyRuntimeOptions{Runtime: runtime, Desired: desiredProviderColumn(), TxMode: migrator.MigrationTxModeNone})
			c.Assert(err, qt.IsNil)

			err = plan.SimulateOnDev(c.Context(), row.opts)

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			observed, err := conn.Reader().ReadSchema()
			c.Assert(err, qt.IsNil)
			c.Assert(observed.Tables[0].Columns, qt.HasLen, 1)
		})
	}
}
