package atlasschema_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/dispatchwitness"
	"ptah.run/migration/migrator"
	"ptah.run/migration/planner"
)

func TestCapturedApplyPlanRefusesNativeSuffixOnSQLExecutor(t *testing.T) {
	for _, mode := range []migrator.MigrationTxMode{migrator.MigrationTxModeNone, migrator.MigrationTxModeAll} {
		t.Run(string(mode), func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(c.TempDir(), "target.db")
			conn := connectSQLite(c, path)
			c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
			plan := planner.RenderedPlan{
				Request: renderer.Request{Target: "sqlite", Capabilities: conn.Info().Capabilities, Nodes: []ast.Node{ast.NewRawSQL("CREATE TABLE prefix (id INTEGER);"), &ast.ExtensionStatement{Payload: transactionOperation{}}}},
				Result:  renderer.Result{Complete: true, ExecutableUnits: true, Fragments: []renderer.Fragment{{SQL: "CREATE TABLE prefix (id INTEGER);"}, {Native: transactionOperation{}}}},
				Dialect: "sqlite",
			}
			captured, err := atlasschema.CaptureApplyPlan(conn.Info(), plan)
			c.Assert(err, qt.IsNil)
			c.Assert(captured.Execute(c.Context(), conn, mode), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sqliteTableExists(c, path, "prefix"), qt.IsFalse)
		})
	}
}

func TestCapturedApplyKeepsBaseAndProvidedCapabilitiesSeparate(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "target.db")
	conn := connectSQLite(c, path)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	info := conn.Info()
	rendered := planner.RenderedPlan{
		Request: renderer.Request{Target: info.Dialect, Capabilities: info.Capabilities.With(capability.Hypertables, true), Nodes: []ast.Node{ast.NewRawSQL("CREATE TABLE prefix (id INTEGER);")}},
		Result:  renderer.Result{Complete: true, ExecutableUnits: true, Fragments: renderer.SQLFragments([]string{"CREATE TABLE prefix (id INTEGER);"})},
		Dialect: info.Dialect,
	}
	captured, err := atlasschema.CaptureApplyPlan(info, rendered)
	c.Assert(err, qt.IsNil)
	c.Assert(captured.Execute(c.Context(), conn, migrator.MigrationTxModeNone), qt.IsNil)
	c.Assert(sqliteTableExists(c, path, "prefix"), qt.IsTrue)
}

func TestCapturedApplyKeepsATrailingSkippedObjectNote(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "target.db")
	conn := connectSQLite(c, path)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	info := conn.Info()
	const note = "-- SQLITE: role app_user is not supported by this target; skipped."
	rendered := planner.RenderedPlan{
		Request: renderer.Request{Target: info.Dialect, Capabilities: info.Capabilities, Nodes: []ast.Node{ast.NewRawSQL("CREATE TABLE prefix (id INTEGER);"), ast.NewRawSQL(note)}},
		Result:  renderer.Result{Complete: true, Fragments: renderer.SQLFragments([]string{"CREATE TABLE prefix (id INTEGER);\n", note + "\n"})},
		Dialect: info.Dialect,
	}

	captured, err := atlasschema.CaptureApplyPlan(info, rendered)

	c.Assert(err, qt.IsNil)
	sql, err := captured.SQL()
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "CREATE TABLE prefix")
	c.Assert(sql, qt.Contains, note)
	c.Assert(captured.Execute(c.Context(), conn, migrator.MigrationTxModeAll), qt.IsNil)
	c.Assert(sqliteTableExists(c, path, "prefix"), qt.IsTrue)
}

// witnessedApply runs statements on conn under a dispatch witness that fills
// evidence, as `schema apply` does, and records the run's error there.
func witnessedApply(ctx context.Context, conn *dbschema.DatabaseConnection, mode migrator.MigrationTxMode, statements []string, evidence *atlasschema.ApplyEvidence) {
	evidence.Err = atlasschema.ApplyStatements(
		dispatchwitness.With(ctx, func() { evidence.Dispatched = true }), conn, mode, statements,
	)
}

// A run that stops before its first statement leaves says so. The connection
// that is already closed is what a BEGIN on a lost connection gets from
// database/sql: an error, with no statement sent.
func TestApplyStatements_ReportsNothingDispatchedBeforeTheFirstStatement(t *testing.T) {
	for _, mode := range []migrator.MigrationTxMode{migrator.MigrationTxModeFile, migrator.MigrationTxModeAll, migrator.MigrationTxModeNone} {
		t.Run("canceled "+string(mode), func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(c.TempDir(), "target.db")
			conn := connectSQLite(c, path)
			c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
			ctx, cancel := context.WithCancel(c.Context())
			cancel()
			var evidence atlasschema.ApplyEvidence

			witnessedApply(ctx, conn, mode, []string{"CREATE TABLE first_table (id INTEGER);"}, &evidence)

			c.Assert(evidence.Err, qt.ErrorIs, context.Canceled)
			c.Assert(evidence.Dispatched, qt.IsFalse)
			c.Assert(atlasschema.NewApplyReport(evidence).Outcome, qt.Equals, atlasschema.ApplyOutcomeFailed)
			c.Assert(sqliteTableExists(c, path, "first_table"), qt.IsFalse)
		})
	}
	for _, mode := range []migrator.MigrationTxMode{migrator.MigrationTxModeFile, migrator.MigrationTxModeAll} {
		t.Run("begin fails "+string(mode), func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(c.TempDir(), "target.db")
			conn := connectSQLite(c, path)
			c.Assert(conn.Close(), qt.IsNil)
			var evidence atlasschema.ApplyEvidence

			witnessedApply(c.Context(), conn, mode, []string{"CREATE TABLE first_table (id INTEGER);"}, &evidence)

			c.Assert(evidence.Err, qt.ErrorMatches, "begin schema apply transaction: .*")
			c.Assert(evidence.Dispatched, qt.IsFalse)
			c.Assert(atlasschema.NewApplyReport(evidence).Outcome, qt.Equals, atlasschema.ApplyOutcomeFailed)
			c.Assert(sqliteTableExists(c, path, "first_table"), qt.IsFalse)
		})
	}
}

// A statement the server refused was still sent. A confirmed transaction
// rollback leaves no applied change; a run without one cannot say that.
func TestApplyStatements_ReportsAFailedStatementAsDispatched(t *testing.T) {
	for _, test := range []struct {
		mode migrator.MigrationTxMode
		want atlasschema.ApplyOutcome
	}{
		{mode: migrator.MigrationTxModeFile, want: atlasschema.ApplyOutcomeFailed},
		{mode: migrator.MigrationTxModeAll, want: atlasschema.ApplyOutcomeFailed},
		{mode: migrator.MigrationTxModeNone, want: atlasschema.ApplyOutcomeUnknown},
	} {
		t.Run(string(test.mode), func(t *testing.T) {
			c := qt.New(t)
			conn := connectSQLite(c, filepath.Join(c.TempDir(), "target.db"))
			c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
			var evidence atlasschema.ApplyEvidence

			witnessedApply(c.Context(), conn, test.mode, []string{"CREATE TABLE broken ("}, &evidence)

			c.Assert(evidence.Err, qt.IsNotNil)
			c.Assert(evidence.Dispatched, qt.IsTrue)
			c.Assert(atlasschema.NewApplyReport(evidence).Outcome, qt.Equals, test.want)
		})
	}
}

func TestApplyStatements_ConfirmedRollbackAfterLaterFailureReportsFailed(t *testing.T) {
	for _, mode := range []migrator.MigrationTxMode{migrator.MigrationTxModeFile, migrator.MigrationTxModeAll} {
		t.Run(string(mode), func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(c.TempDir(), "target.db")
			conn := connectSQLite(c, path)
			c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
			var evidence atlasschema.ApplyEvidence

			witnessedApply(c.Context(), conn, mode, []string{"CREATE TABLE first_table (id INTEGER);", "CREATE TABLE broken ("}, &evidence)

			c.Assert(evidence.Err, qt.IsNotNil)
			c.Assert(evidence.Dispatched, qt.IsTrue)
			report := atlasschema.NewApplyReport(evidence)
			c.Assert(report.Outcome, qt.Equals, atlasschema.ApplyOutcomeFailed)
			c.Assert(report.Failure, qt.DeepEquals, &atlasschema.ApplyFailure{Statement: 2, Statements: 2})
			c.Assert(sqliteTableExists(c, path, "first_table"), qt.IsFalse)
		})
	}
}

// The second statement failing leaves the first applied, and the report must
// not read the failure as the end of a run that sent nothing.
func TestApplyStatements_ReportsALaterFailureAsDispatched(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "target.db")
	conn := connectSQLite(c, path)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	var evidence atlasschema.ApplyEvidence

	witnessedApply(c.Context(), conn, migrator.MigrationTxModeNone, []string{"CREATE TABLE first_table (id INTEGER);", "CREATE TABLE broken ("}, &evidence)

	c.Assert(evidence.Err, qt.IsNotNil)
	c.Assert(evidence.Dispatched, qt.IsTrue)
	c.Assert(atlasschema.NewApplyReport(evidence).Outcome, qt.Equals, atlasschema.ApplyOutcomeUnknown)
	c.Assert(sqliteTableExists(c, path, "first_table"), qt.IsTrue)
}

// A run that applied a unit and then could not begin the transaction of the
// next one reached the database, so the failure to begin does not clear what the
// witness heard. `schema apply` opens one transaction for its whole batch, so
// this is the rule a second batch would have to keep.
func TestApplyStatements_ARefusedBeginAfterAnAppliedUnitStaysDispatched(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "target.db")
	conn := connectSQLite(c, path)
	var evidence atlasschema.ApplyEvidence

	witnessedApply(c.Context(), conn, migrator.MigrationTxModeFile, []string{"CREATE TABLE first_table (id INTEGER);"}, &evidence)
	c.Assert(evidence.Err, qt.IsNil)
	c.Assert(evidence.Dispatched, qt.IsTrue)
	c.Assert(conn.Close(), qt.IsNil)
	witnessedApply(c.Context(), conn, migrator.MigrationTxModeFile, []string{"CREATE TABLE second_table (id INTEGER);"}, &evidence)

	c.Assert(evidence.Err, qt.ErrorMatches, "begin schema apply transaction: .*")
	c.Assert(evidence.Dispatched, qt.IsTrue)
	c.Assert(atlasschema.NewApplyReport(evidence).Outcome, qt.Equals, atlasschema.ApplyOutcomeUnknown)
	c.Assert(sqliteTableExists(c, path, "first_table"), qt.IsTrue)
	c.Assert(sqliteTableExists(c, path, "second_table"), qt.IsFalse)
}

// The refusal says what the operator can do, and keeps the target's reason where
// a caller can test for it.
func TestTransactionUnavailableError(t *testing.T) {
	c := qt.New(t)
	err := &atlasschema.TransactionUnavailableError{
		Mode: migrator.MigrationTxModeAll,
		Err:  fmt.Errorf("%w: the provider offers no transactions", ptaherr.ErrUnsupportedFeature),
	}

	c.Assert(err, qt.ErrorMatches, `transaction mode "all" needs a transaction this target cannot open: `+
		`unsupported feature: the provider offers no transactions; rerun with --tx-mode none, which commits each statement as it runs`)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
}
