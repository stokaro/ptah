//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

func TestProviderHistoryDataTransactionsDoNotAuthorizeDDL(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	for _, test := range []struct {
		name   string
		finish func(dbschema.HistoryTransaction) error
		rows   int
	}{
		{"commit", dbschema.HistoryTransaction.Commit, 1},
		{"rollback", dbschema.HistoryTransaction.Rollback, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, _ := transactionConnection(c, host, "")
			c.Assert(conn.Info().Capabilities.Has(capability.TransactionalDDL), qt.IsFalse)
			_, err := conn.SchemaWriter().BeginTransaction(c.Context())
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			store, err := conn.HistoryStore()
			c.Assert(err, qt.IsNil)
			c.Assert(store.Prepare(c.Context(), providerHistoryTable()), qt.IsNil)
			owner, ok := store.(dbschema.HistoryTransactor)
			c.Assert(ok, qt.IsTrue)
			tx, err := owner.BeginHistoryTransaction(c.Context(), dbschema.HistoryIsolationDefault)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { _ = tx.Rollback() })
			_, exposesSQL := tx.(catalog.SchemaExecutor)
			c.Assert(exposesSQL, qt.IsFalse)
			row := providerHistoryRecord("1")
			c.Assert(tx.CompareAndSwap(c.Context(), providerHistoryTable(), nil, &row), qt.IsNil)
			c.Assert(readProviderHistory(c, tx).Records, qt.DeepEquals, []dbschema.HistoryRecord{row})
			c.Assert(test.finish(tx), qt.IsNil)
			c.Assert(readProviderHistory(c, store).Records, qt.HasLen, test.rows)
			_, err = tx.Read(c.Context(), providerHistoryTable())
			c.Assert(err, qt.ErrorMatches, "provider transaction has finished execution")
		})
	}
}

func TestProviderHistoryDataTransactionsRefuseInvalidLifetimes(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, _ := transactionConnection(c, host, "")
	store, err := conn.HistoryStore()
	c.Assert(err, qt.IsNil)
	owner, ok := store.(dbschema.HistoryTransactor)
	c.Assert(ok, qt.IsTrue)
	conn.SchemaWriter().SetDryRun(true)
	_, err = owner.BeginHistoryTransaction(c.Context(), dbschema.HistoryIsolationDefault)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	conn.SchemaWriter().SetDryRun(false)
	tx, err := owner.BeginHistoryTransaction(c.Context(), dbschema.HistoryIsolationDefault)
	c.Assert(err, qt.IsNil)
	nested, ok := tx.(dbschema.HistoryTransactor)
	c.Assert(ok, qt.IsTrue)
	_, err = nested.BeginHistoryTransaction(c.Context(), dbschema.HistoryIsolationDefault)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(tx.Rollback(), qt.IsNil)
	c.Assert(conn.Close(), qt.IsNil)
	_, err = owner.BeginHistoryTransaction(c.Context(), dbschema.HistoryIsolationDefault)
	c.Assert(err, qt.ErrorMatches, "provider session is closed")
}

func providerBaselineRunner(conn *dbschema.DatabaseConnection) *migrator.Migrator {
	first := migrator.CreateMigrationFromSQL(2, "second", "CREATE TABLE baseline_body_must_not_run (id INTEGER);", "")
	second := migrator.CreateMigrationFromSQL(10, "tenth", "SELECT 10; SELECT 11;", "")
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(first, second)).
		WithMigrationsTable("main", "_revisions").WithMigrationLog(false).WithoutMigrationLock()
}

func TestProviderHistoryBaselineUsesDataTransactions(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, _ := transactionConnection(c, host, "")
	runner := providerBaselineRunner(conn)
	c.Assert(runner.Baseline(c.Context(), 2), qt.IsNil)
	rows, err := runner.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 1)
	c.Assert(rows[0].Version, qt.Equals, int64(2))
	c.Assert(rows[0].Applied, qt.Equals, 1)
	c.Assert(rows[0].Dirty, qt.IsFalse)
	c.Assert(rows[0].Checksum, qt.Not(qt.Equals), "")
	c.Assert(runner.BaselineWithOptions(c.Context(), migrator.BaselineOptions{Version: 10, Force: true}), qt.IsNil)
	rows, err = runner.GetRevisions(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 2)
	c.Assert(rows[1].Version, qt.Equals, int64(10))
	c.Assert(rows[1].Applied, qt.Equals, 2)
	c.Assert(rows[1].Dirty, qt.IsFalse)
	// Creation succeeds only if baseline never executed the migration body.
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), "CREATE TABLE baseline_body_must_not_run (id INTEGER);"), qt.IsNil)
}

func TestProviderHistoryBaselineRefusalsPreserveRecords(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	for _, test := range []struct {
		name    string
		opts    migrator.BaselineOptions
		state   string
		message string
	}{
		{"nonempty", migrator.BaselineOptions{Version: 10}, "applied", "schema migrations table is not empty;.*"},
		{"above baseline", migrator.BaselineOptions{Version: 2, Force: true}, "applied", ".*contains revisions above baseline version 2;.*"},
		{"dirty despite force", migrator.BaselineOptions{Version: 10, Force: true}, "pending", ".*dirty.*"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, _ := transactionConnection(c, host, "")
			runner := providerBaselineRunner(conn)
			c.Assert(runner.Initialize(c.Context()), qt.IsNil)
			store, err := conn.HistoryStore()
			c.Assert(err, qt.IsNil)
			row := providerHistoryRecord("10")
			row.State = test.state
			c.Assert(store.CompareAndSwap(c.Context(), providerHistoryTable(), nil, &row), qt.IsNil)
			before := readProviderHistory(c, store)
			c.Assert(runner.BaselineWithOptions(c.Context(), test.opts), qt.ErrorMatches, test.message)
			c.Assert(readProviderHistory(c, store), qt.DeepEquals, before)
		})
	}
}

func TestProviderHistoryBaselinePreservesUncertainOutcome(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	for _, test := range []struct {
		fault string
		rows  int
	}{
		{"lost-history", 0},
		{"lost-history-last", 0},
		{"lost-before-commit", 0},
		{"lost-commit", 2},
	} {
		t.Run(test.fault, func(t *testing.T) {
			c := qt.New(t)
			conn, address := transactionConnection(c, host, test.fault)
			runner := providerBaselineRunner(conn)
			c.Assert(runner.Baseline(c.Context(), 10), qt.ErrorIs, ptaherr.ErrOutcomeUnknown)
			store, err := conn.HistoryStore()
			c.Assert(err, qt.IsNil)
			// Read through the same session proves the failed operation released its handle.
			c.Assert(readProviderHistory(c, store).Records, qt.HasLen, test.rows)
			c.Assert(conn.Close(), qt.IsNil)
			fresh := connectTransactionAddress(c, host, address)
			rows, err := providerBaselineRunner(fresh).GetRevisions(c.Context())
			c.Assert(err, qt.IsNil)
			c.Assert(rows, qt.HasLen, test.rows)
		})
	}
}
