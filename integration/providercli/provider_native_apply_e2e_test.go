//go:build integration

package providercli_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/ast"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/migrator"
	"ptah.run/migration/planner"
)

func nativeExecutionPlan(c *qt.C, runtime *engine.Runtime, conn *dbschema.DatabaseConnection, transaction plangraph.Transaction, names ...string) planner.RenderedPlan {
	c.Helper()
	nodes := []ast.Node{&ast.CreateTableNode{Name: "common", Columns: []*ast.ColumnNode{{Name: "id", Type: "INTEGER", Primary: true}}}}
	var features []string
	if conn.Info().Capabilities.Has(capability.TransactionalDDL) {
		features = append(features, provider.TransactionsFeature)
	}
	for _, name := range names {
		nodes = append(nodes, &ast.ExtensionStatement{Payload: capturedOperation(c, runtime, name, `{"action":"create"}`, features...), Transaction: transaction})
	}
	request := renderer.Request{Target: "fixture", Capabilities: conn.Info().Capabilities, Nodes: nodes}
	result, err := runtime.Render(c.Context(), request)
	c.Assert(err, qt.IsNil)
	return planner.RenderedPlan{Request: request, Result: result, Dialect: "fixture"}
}

func TestProviderNativeApplyExecutesMixedPlan(t *testing.T) {
	for _, test := range []struct {
		name        string
		profile     string
		mode        migrator.MigrationTxMode
		transaction plangraph.Transaction
	}{
		{"outside transaction", "native", migrator.MigrationTxModeNone, plangraph.TransactionForbidden},
		{"inside transaction", "native-transactional", migrator.MigrationTxModeAll, plangraph.TransactionRequired},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			host, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=" + test.profile})
			conn, _ := transactionConnection(c, host, "")
			plan := nativeExecutionPlan(c, runtime, conn, test.transaction, "records")
			c.Assert(applyCapturedPlan(c.Context(), conn, test.mode, plan), qt.IsNil)
			c.Assert(providerTablePresent(c, conn, "common"), qt.IsTrue)
			c.Assert(providerTablePresent(c, conn, "records"), qt.IsTrue)
		})
	}
}

func TestProviderNativeApplyRefusesBeforeSQLPrefix(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native"})
	canceled, cancel := context.WithCancel(c.Context())
	cancel()
	unchanged := func(*planner.RenderedPlan) {}
	for _, test := range []struct {
		name        string
		ctx         context.Context
		mode        migrator.MigrationTxMode
		requirement plangraph.Transaction
		change      func(*planner.RenderedPlan)
		want        error
	}{
		{"forbidden transaction", c.Context(), migrator.MigrationTxModeAll, plangraph.TransactionForbidden, unchanged, ptaherr.ErrUnsupportedFeature},
		{"required transaction", c.Context(), migrator.MigrationTxModeNone, plangraph.TransactionRequired, unchanged, ptaherr.ErrUnsupportedFeature},
		{"invalid suffix", c.Context(), migrator.MigrationTxModeNone, plangraph.TransactionForbidden, func(p *planner.RenderedPlan) { p.Result.Fragments[1].SQL = "SELECT 1;" }, renderer.ErrInvalidResult},
		{"target mismatch", c.Context(), migrator.MigrationTxModeNone, plangraph.TransactionForbidden, func(p *planner.RenderedPlan) { p.Request.Target = "sqlite" }, renderer.ErrInvalidResult},
		{"unknown capabilities", c.Context(), migrator.MigrationTxModeNone, plangraph.TransactionForbidden, func(p *planner.RenderedPlan) { p.Request.Capabilities = capability.Capabilities{} }, renderer.ErrInvalidResult},
		{"canceled", canceled, migrator.MigrationTxModeNone, plangraph.TransactionForbidden, unchanged, context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, _ := transactionConnection(c, host, "")
			plan := nativeExecutionPlan(c, runtime, conn, test.requirement, "records")
			test.change(&plan)
			c.Assert(applyCapturedPlan(test.ctx, conn, test.mode, plan), qt.ErrorIs, test.want)
			c.Assert(providerTablePresent(c, conn, "common"), qt.IsFalse)
			c.Assert(providerTablePresent(c, conn, "records"), qt.IsFalse)
		})
	}
}

func TestProviderNativeApplyBindsFrozenSelection(t *testing.T) {
	c := qt.New(t)
	_, foreign := yamlProviderCommand(c, "operation-models")
	host, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native"})
	conn, _ := transactionConnection(c, host, "")
	plan := nativeExecutionPlan(c, runtime, conn, plangraph.TransactionForbidden, "records")
	plan.Result.Fragments[1].Native = capturedOperation(c, foreign, "records", `{"action":"create"}`)
	c.Assert(applyCapturedPlan(c.Context(), conn, migrator.MigrationTxModeNone, plan), qt.IsNotNil)
	c.Assert(providerTablePresent(c, conn, "common"), qt.IsFalse)
}

func TestProviderNativeApplyReportsPrefixWithoutRetry(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native"})
	for _, test := range []struct {
		name, fault string
		names       []string
		applied     int
	}{
		{"partial response", "partial", []string{"records"}, 1},
		{"uncertain suffix", "", []string{"records", "records"}, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, _ := transactionConnection(c, host, test.fault)
			plan := nativeExecutionPlan(c, runtime, conn, plangraph.TransactionForbidden, test.names...)
			err := applyCapturedPlan(c.Context(), conn, migrator.MigrationTxModeNone, plan)
			failure, ok := errors.AsType[*dbschema.UnitExecutionError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(failure.Applied, qt.Equals, test.applied)
			c.Assert(failure.Total, qt.Equals, len(test.names)+1)
			c.Assert(providerTablePresent(c, conn, "common"), qt.IsTrue)
			c.Assert(providerTablePresent(c, conn, "records"), qt.Equals, test.applied == 2)
			c.Assert(errors.Is(err, ptaherr.ErrOutcomeUnknown), qt.Equals, test.fault == "")
		})
	}
}

func TestProviderNativeApplyRollsBackFailedBatch(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native-transactional"})
	conn, _ := transactionConnection(c, host, "")
	plan := nativeExecutionPlan(c, runtime, conn, plangraph.TransactionRequired, "records", "records")
	c.Assert(applyCapturedPlan(c.Context(), conn, migrator.MigrationTxModeAll, plan), qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	c.Assert(providerTablePresent(c, conn, "common"), qt.IsFalse)
	c.Assert(providerTablePresent(c, conn, "records"), qt.IsFalse)
}

func TestProviderNativeApplyDryRunValidatesWithoutWriting(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native"})
	conn, _ := transactionConnection(c, host, "")
	plan := nativeExecutionPlan(c, runtime, conn, plangraph.TransactionForbidden, "records")
	conn.SchemaWriter().SetDryRun(true)
	c.Assert(applyCapturedPlan(c.Context(), conn, migrator.MigrationTxModeNone, plan), qt.IsNil)
	plan.Result.Fragments[1].SQL = "SELECT 1;"
	c.Assert(applyCapturedPlan(c.Context(), conn, migrator.MigrationTxModeNone, plan), qt.IsNotNil)
	conn.SchemaWriter().SetDryRun(false)
	c.Assert(providerTablePresent(c, conn, "common"), qt.IsFalse)
	c.Assert(providerTablePresent(c, conn, "records"), qt.IsFalse)
}

func TestProviderNativeApplyRefusalLeavesSessionUsable(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native"})
	conn, _ := transactionConnection(c, host, "")
	plan := nativeExecutionPlan(c, runtime, conn, plangraph.TransactionForbidden, "records")
	units := plan.Result.Fragments
	units[1].Native = capturedOperation(c, runtime, "records", `{"action":"create","transaction":"required"}`)
	executor, ok := conn.Writer().(dbschema.UnitExecutor)
	c.Assert(ok, qt.IsTrue)
	err := executor.ExecuteUnits(c.Context(), units)
	failure, ok := errors.AsType[*dbschema.UnitExecutionError](err)
	c.Assert(ok, qt.IsTrue)
	c.Assert(failure.Applied, qt.Equals, 0)
	c.Assert(failure.Total, qt.Equals, 2)
	c.Assert(err, qt.ErrorIs, provider.ErrExecution)
	c.Assert(err, qt.Not(qt.ErrorIs), ptaherr.ErrOutcomeUnknown)
	c.Assert(providerTablePresent(c, conn, "common"), qt.IsFalse)
	units[1].Native = capturedOperation(c, runtime, "records", `{"action":"create"}`)
	c.Assert(executor.ExecuteUnits(c.Context(), units), qt.IsNil)
	c.Assert(providerTablePresent(c, conn, "records"), qt.IsTrue)
}

func TestProviderNativeTransactionPreflightAllowsCorrection(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native-transactional"})
	conn, _ := transactionConnection(c, host, "")
	plan := nativeExecutionPlan(c, runtime, conn, plangraph.TransactionRequired, "records")
	tx, err := conn.SchemaWriter().BeginTransaction(c.Context())
	c.Assert(err, qt.IsNil)
	executor, ok := tx.(dbschema.UnitExecutor)
	c.Assert(ok, qt.IsTrue)
	units := plan.Result.Fragments
	units[1].SQL = "SELECT 1;"
	c.Assert(executor.ExecuteUnits(c.Context(), units), qt.ErrorIs, renderer.ErrInvalidResult)
	units[1].SQL = ""
	c.Assert(executor.ExecuteUnits(c.Context(), units), qt.IsNil)
	c.Assert(tx.Commit(), qt.IsNil)
	c.Assert(providerTablePresent(c, conn, "common"), qt.IsTrue)
	c.Assert(providerTablePresent(c, conn, "records"), qt.IsTrue)
}

func TestProviderNativeTransactionFailurePreventsCommit(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderEnvironment(c, "operation-models", []string{"FIXTURE_RENDER_MODE=native-transactional"})
	conn, _ := transactionConnection(c, host, "")
	plan := nativeExecutionPlan(c, runtime, conn, plangraph.TransactionRequired, "records", "records")
	tx, err := conn.SchemaWriter().BeginTransaction(c.Context())
	c.Assert(err, qt.IsNil)
	executor, ok := tx.(dbschema.UnitExecutor)
	c.Assert(ok, qt.IsTrue)
	c.Assert(executor.ExecuteUnits(c.Context(), plan.Result.Fragments), qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	c.Assert(tx.Commit(), qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
	c.Assert(tx.Rollback(), qt.IsNil)
	c.Assert(providerTablePresent(c, conn, "common"), qt.IsFalse)
	c.Assert(providerTablePresent(c, conn, "records"), qt.IsFalse)
}

func applyCapturedPlan(ctx context.Context, conn *dbschema.DatabaseConnection, mode migrator.MigrationTxMode, rendered planner.RenderedPlan) error {
	plan, err := atlasschema.CaptureApplyPlan(conn.Info(), rendered)
	if err != nil {
		return err
	}
	return plan.Execute(ctx, conn, mode)
}
