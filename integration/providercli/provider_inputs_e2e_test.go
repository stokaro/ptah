//go:build integration

package providercli_test

import (
	"context"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/plangraph"
	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/inputapproval"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

const executionInputName = "PTAH_FIXTURE_EXECUTION_INPUT"

func TestProviderInputValuesAreResolvedOnlyAtDispatch(t *testing.T) {
	for _, test := range []struct {
		name, mode, value string
	}{
		{"nonempty", "stable", "fixture-private-value"},
		{"present empty", "empty", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(executionInputName, "unused-during-planning")
			host, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native", "FIXTURE_INPUT_MODE=" + test.mode})
			conn, _ := transactionConnection(c, host, "")
			rendered := nativeExecutionPlanWithFeatures(c, runtime, conn, plangraph.TransactionForbidden, []string{provider.SensitiveInputsFeature, provider.HistoryFeature, provider.StatementsFeature}, "needs_input")
			c.Assert(rendered.Result.Fragments[0].Inputs, qt.HasLen, 0)
			c.Assert(rendered.Result.Fragments[1].Inputs, qt.DeepEquals, []renderer.InputReference{{Source: renderer.InputEnvironment, Name: executionInputName}})
			plan, err := atlasschema.CaptureApplyPlan(conn.Info(), rendered)
			c.Assert(err, qt.IsNil)
			c.Assert(plan.Preflight(c.Context(), conn, migrator.MigrationTxModeNone), qt.IsNil)
			conn.SchemaWriter().SetDryRun(true)
			c.Assert(plan.Execute(c.Context(), conn, migrator.MigrationTxModeNone), qt.IsNil)
			c.Assert(providerTablePresent(c, conn, "common"), qt.IsFalse)
			conn.SchemaWriter().SetDryRun(false)
			t.Setenv(executionInputName, test.value)
			c.Assert(plan.Execute(c.Context(), conn, migrator.MigrationTxModeNone), qt.IsNil)
			c.Assert(providerTablePresent(c, conn, "needs_input"), qt.IsTrue)
		})
	}
}

func TestProviderInputChangesRefuseTheWholeBatch(t *testing.T) {
	for _, test := range []struct {
		name, mode string
		change     func(*renderer.Fragment)
	}{
		{"description changed", "changed", func(*renderer.Fragment) {}},
		{"batch requests additional input", "batch-extra", func(*renderer.Fragment) {}},
		{"approval stripped", "stable", func(fragment *renderer.Fragment) { fragment.Inputs = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(executionInputName, "fixture-private-value")
			host, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native", "FIXTURE_INPUT_MODE=" + test.mode})
			conn, _ := transactionConnection(c, host, "")
			rendered := nativeExecutionPlanWithFeatures(c, runtime, conn, plangraph.TransactionForbidden, []string{provider.SensitiveInputsFeature, provider.HistoryFeature, provider.StatementsFeature}, "needs_input")
			test.change(&rendered.Result.Fragments[1])
			err := applyCapturedPlan(c.Context(), conn, migrator.MigrationTxModeNone, rendered)
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Not(qt.Contains), "fixture-private-value")
			c.Assert(providerTablePresent(c, conn, "common"), qt.IsFalse)
			c.Assert(providerTablePresent(c, conn, "needs_input"), qt.IsFalse)
		})
	}
}

func TestProviderInputCancellationRefusesBeforeDispatch(t *testing.T) {
	c := qt.New(t)
	t.Setenv(executionInputName, "fixture-private-value")
	host, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native", "FIXTURE_INPUT_MODE=stable"})
	conn, _ := transactionConnection(c, host, "")
	rendered := nativeExecutionPlanWithFeatures(c, runtime, conn, plangraph.TransactionForbidden, []string{provider.SensitiveInputsFeature, provider.HistoryFeature, provider.StatementsFeature}, "needs_input")
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	executor := conn.Writer().(dbschema.UnitExecutor)
	c.Assert(executor.ExecuteUnits(ctx, rendered.Result.Fragments), qt.ErrorIs, context.Canceled)
	c.Assert(providerTablePresent(c, conn, "common"), qt.IsFalse)
}

func TestProviderMissingInputRefusesBeforeSQLPrefix(t *testing.T) {
	c := qt.New(t)
	t.Setenv(executionInputName, "restore-after-test")
	c.Assert(os.Unsetenv(executionInputName), qt.IsNil)
	host, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native", "FIXTURE_INPUT_MODE=stable"})
	conn, _ := transactionConnection(c, host, "")
	rendered := nativeExecutionPlanWithFeatures(c, runtime, conn, plangraph.TransactionForbidden, []string{provider.SensitiveInputsFeature, provider.HistoryFeature, provider.StatementsFeature}, "needs_input")
	plan, err := atlasschema.CaptureApplyPlan(conn.Info(), rendered)
	c.Assert(err, qt.IsNil)
	c.Assert(plan.Preflight(c.Context(), conn, migrator.MigrationTxModeNone), qt.IsNil)
	err = plan.Execute(c.Context(), conn, migrator.MigrationTxModeNone)
	c.Assert(err, qt.ErrorMatches, `provider execution input environment variable "PTAH_FIXTURE_EXECUTION_INPUT" is not set`)
	c.Assert(providerTablePresent(c, conn, "common"), qt.IsFalse)
	c.Assert(providerTablePresent(c, conn, "needs_input"), qt.IsFalse)
}

func TestProviderMigrationInputsReachApprovalBeforeUpAndDown(t *testing.T) {
	c := qt.New(t)
	t.Setenv(executionInputName, "fixture-private-value")
	host, _ := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native", "FIXTURE_INPUT_MODE=stable"})
	conn, _ := transactionConnection(c, host, "")
	migration := migrator.CreateMigrationFromSQL(1, "input", `CREATE TABLE "needs_input" ("id" INTEGER);`, `DROP TABLE "needs_input";`)
	migration.DownTxMode = migrationfile.FileTxModeNone
	runner := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).WithTransactionMode(migrator.MigrationTxModeNone).
		WithMigrationsTable("main", "_input_revisions").WithoutMigrationLock()
	c.Assert(runner.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{Preflight: func(_ context.Context, plan migrator.MigrationPlan) error {
		c.Assert(plan.Inputs, qt.HasLen, 1)
		c.Assert(plan.Inputs[0].Inputs[0].Name, qt.Equals, executionInputName)
		c.Assert(providerTablePresent(c, conn, "needs_input"), qt.IsFalse)
		return nil
	}}), qt.IsNil)
	c.Assert(providerTablePresent(c, conn, "needs_input"), qt.IsTrue)
	c.Assert(runner.MigrateDownToWithPreflight(c.Context(), 0, func(_ context.Context, plan migrator.MigrationPlan) error {
		c.Assert(plan.Inputs, qt.HasLen, 1)
		c.Assert(plan.Inputs[0].Inputs[0].Name, qt.Equals, executionInputName)
		return nil
	}), qt.IsNil)
	c.Assert(providerTablePresent(c, conn, "needs_input"), qt.IsFalse)
}

func TestProviderOperatorInputsUseOnlyApprovedWriteUnits(t *testing.T) {
	c := qt.New(t)
	t.Setenv(executionInputName, "fixture-private-value")
	host, _ := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native", "FIXTURE_INPUT_MODE=stable"})
	conn, _ := transactionConnection(c, host, "")
	operator, closeOperator, err := conn.OperatorStatements()
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(closeOperator(), qt.IsNil) })
	const sql = `CREATE TABLE "needs_input" ("id" INTEGER);`
	_, err = operator.ExecContext(c.Context(), sql)
	c.Assert(err, qt.IsNotNil)
	c.Assert(providerTablePresent(c, conn, "needs_input"), qt.IsFalse)
	prepared, err := conn.Writer().(dbschema.UnitInputPreparer).PrepareUnitInputs(c.Context(), renderer.SQLFragments([]string{sql}))
	c.Assert(err, qt.IsNil)
	approved, err := inputapproval.With(c.Context(), "fixture", prepared)
	c.Assert(err, qt.IsNil)
	rows, err := operator.QueryContext(approved, sql) //nolint:rowserrcheck // A refused query must return no iterator to inspect or close.
	c.Assert(err, qt.IsNotNil)
	c.Assert(rows, qt.IsNil)
	c.Assert(providerTablePresent(c, conn, "needs_input"), qt.IsFalse)
	_, err = operator.ExecContext(approved, sql)
	c.Assert(err, qt.IsNil)
	c.Assert(providerTablePresent(c, conn, "needs_input"), qt.IsTrue)
}
