//go:build integration

package providercli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/testutils"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// The plan these tests rehearse adds a column to a table the target holds,
// which the operation-models fixture plans as one native operation. The
// fixture cleans and guards a dev database, so a rehearsal can claim one. The
// expected state comes from reading the database files back.

// rehearsalCLI is the target with the table, a desired schema that adds the
// column, and a dev database file beside them.
type rehearsalCLI struct {
	providerCLI
	devPath string
}

func newRehearsalCLI(c *qt.C) rehearsalCLI {
	c.Helper()
	spec := nativeCommandSpec(c, testutils.FixtureRelease{})
	spec.Environment = append(spec.Environment, "FIXTURE_DEV_SERVICES=1")
	return rehearsalCLI{providerCLI: nativeCommandFixtureFor(c, spec), devPath: filepath.Join(c.TempDir(), "dev.db")}
}

// A plan that holds a native operation is rehearsed on the dev database: the
// target's table is created there, the change is planned again for the dev
// database and runs through the provider, the target gets the column, and the
// dev database is handed back empty.
func TestProviderNativeRehearsalRunsThePlanOnTheDevDatabase_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newRehearsalCLI(c)

	applied := f.success(c, "apply", "--schema-file", f.schemaPath, "--tx-mode", "none", "--dev-url", fixtureAddress("", f.devPath),
		"--auto-approve", "--json")

	var report atlasschema.ApplyReport
	c.Assert(json.Unmarshal([]byte(applied), &report), qt.IsNil)
	c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeApplied)
	c.Assert(report.Units, qt.HasLen, 1)
	c.Assert(report.Units[0].Native, qt.IsNotNil)
	c.Assert(report.Units[0].Native.Kind, qt.Equals, operationModel)
	c.Assert(f.columns(c), qt.DeepEquals, []string{"id", "added"})
	c.Assert(tables(c, f.devPath), qt.HasLen, 0)
}

// A rehearsal that fails on the dev database refuses the apply, and the
// target is left as it was. The dev database's provider runs nothing, so the
// rehearsal fails while it recreates the target's table there.
func TestProviderNativeRehearsalFailureRefusesTheApply_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := newRehearsalCLI(c)

	_, diagnostics, code := f.run(c, c.Context(), "apply", "--schema-file", f.schemaPath, "--tx-mode", "none",
		"--dev-url", fixtureAddress("", f.devPath)+"?fault=reject", "--auto-approve")

	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(diagnostics, qt.Contains, "dev database simulation failed during baseline: ")
	c.Assert(diagnostics, qt.Contains, "the plan was not applied to the target database")
	c.Assert(f.columns(c), qt.DeepEquals, []string{"id"})
}

// A dev database that is the target is refused before anything runs, whether
// or not the operator declared its server disposable, and the target is left
// as it was.
func TestProviderNativeRehearsalRefusesTheTargetAsItsDevDatabase_FailurePath(t *testing.T) {
	for _, disposable := range []string{"0", "1"} {
		t.Run(devdocker.DisposableServerEnvVar+"="+disposable, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv(devdocker.DisposableServerEnvVar, disposable)
			f := newRehearsalCLI(c)

			_, diagnostics, code := f.run(c, c.Context(), "apply", "--schema-file", f.schemaPath, "--tx-mode", "none",
				"--dev-url", f.address, "--auto-approve")

			c.Assert(code, qt.Not(qt.Equals), 0)
			c.Assert(diagnostics, qt.Contains, "--dev-url must not point at the target database")
			c.Assert(f.columns(c), qt.DeepEquals, []string{"id"})
		})
	}
}

// A replay runs a migration that reads an input the way migrate up runs it:
// the input is approved before the first statement, and its value is bound
// when the statement is dispatched. Without the approval the provider refuses
// the statement, because the reference it discovers differs from the none
// the statement was approved with.
func TestProviderReplayApprovesTheInputsAMigrationReads_HappyPath(t *testing.T) {
	c := qt.New(t)
	t.Setenv(executionInputName, "fixture-private-value")
	host, _ := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native", "FIXTURE_INPUT_MODE=stable"})
	conn, _ := transactionConnection(c, host, "")
	migration := migrator.CreateMigrationFromSQL(1, "input", `CREATE TABLE "needs_input" ("id" INTEGER);`, `DROP TABLE "needs_input";`)
	migration.UpTxMode = migrationfile.FileTxModeNone

	err := migration.UpForReplay(c.Context(), conn)

	c.Assert(err, qt.IsNil)
	c.Assert(providerTablePresent(c, conn, "needs_input"), qt.IsTrue)
}

// A rehearsal holds the plan it makes for the dev database to the target's,
// operation by operation, and refuses one that differs: it would pass on a
// change the apply does not make. The desired state loaded for the dev
// database here declares a second column the target's plan does not add.
func TestProviderNativeRehearsalRefusesAPlanThatIsNotTheTargets_FailurePath(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native", "FIXTURE_DEV_SERVICES=1", "FIXTURE_STATEMENT_STYLE=honored"})
	ctx, err := dbschema.WithNativeConnections(c.Context(), host.Connections())
	c.Assert(err, qt.IsNil)
	conn := observedConnection(c, host, "planned")
	devPath := filepath.Join(c.TempDir(), "dev.db")
	other := desiredProviderColumn()
	other.Fields = append(other.Fields, schemamodel.Field{StructName: "Planned", Name: "other", Type: "REAL", Nullable: true})
	plan, err := atlasschema.PrepareApply(ctx, conn, atlasschema.ApplyRuntimeOptions{
		Runtime: runtime, Desired: desiredProviderColumn(), TxMode: migrator.MigrationTxModeNone,
		DesiredFor: func(context.Context, string) (*schemamodel.Database, error) { return other, nil },
	})
	c.Assert(err, qt.IsNil)

	err = plan.SimulateOnDev(ctx, atlasschema.SimulateOptions{DevURL: fixtureAddress("", devPath)})

	c.Assert(err, qt.ErrorMatches, `dev database simulation failed during plan: the plan made for the dev database is not the plan for the target: `+
		`the target's plan has 1 units and the dev database's 2; unit 2 is .* on the dev database; the plan was not applied to the target database`)
	observed, err := conn.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(observed.Tables[0].Columns, qt.HasLen, 1)
	c.Assert(tables(c, devPath), qt.HasLen, 0)
}

// The compatibility surface rehearses a native plan through the same shared
// rehearsal: the target gets the column and the dev database is handed back
// empty, and a dev database whose provider runs nothing refuses the apply.
func TestProviderCompatNativeRehearsalRunsThePlanOnTheDevDatabase(t *testing.T) {
	c := qt.New(t)
	f := newRehearsalCLI(c)
	failing := fixtureAddress("", f.devPath) + "?fault=reject"

	refusedOut, refused, refusedCode := compatSchemaApply(c, f, "--dev-url", failing)
	refusedColumns := f.columns(c)
	appliedOut, applied, appliedCode := compatSchemaApply(c, f, "--dev-url", fixtureAddress("", f.devPath))

	c.Assert(refusedCode, qt.Not(qt.Equals), 0, qt.Commentf("%s", refusedOut))
	c.Assert(refused, qt.Contains, "dev database simulation failed during baseline: ")
	c.Assert(refusedColumns, qt.DeepEquals, []string{"id"})
	c.Assert(appliedCode, qt.Equals, 0, qt.Commentf("%s\n%s", appliedOut, applied))
	c.Assert(f.columns(c), qt.DeepEquals, []string{"id", "added"})
	c.Assert(tables(c, f.devPath), qt.HasLen, 0)
}

// compatSchemaApply runs `ptah-compat schema apply` of the fixture's schema
// file against its database, with the manifest selected the only way that
// surface selects one.
func compatSchemaApply(c *qt.C, f rehearsalCLI, args ...string) (stdout, stderr string, exitCode int) {
	c.Helper()
	c.Setenv("PTAH_PROVIDER_CONFIG", f.manifestPath)
	cmd := atlas.NewCompatCommandWithPolicy("ptah-compat", atlascompatpolicy.Full())
	var out, diagnostics bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&diagnostics)
	cmd.SetIn(strings.NewReader(""))
	exitCode = root.RunContext(c.Context(), cmd, append([]string{"schema", "apply", "--url", f.address,
		"--to", "file://" + filepath.ToSlash(f.schemaPath), "--tx-mode", "none", "--auto-approve"}, args...)...)
	f.assertStopped(c)
	return out.String(), diagnostics.String(), exitCode
}
