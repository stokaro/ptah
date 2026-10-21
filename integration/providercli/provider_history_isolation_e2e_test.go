//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
)

func TestProviderHistorySerializableSnapshotRefusesStaleRangeWrite(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	first, address := transactionConnection(c, host, "")
	c.Assert(first.Writer().ExecuteSQL(c.Context(), "PRAGMA journal_mode=WAL;"), qt.IsNil)
	store, err := first.HistoryStore()
	c.Assert(err, qt.IsNil)
	c.Assert(store.Prepare(c.Context(), providerHistoryTable()), qt.IsNil)
	owner, ok := store.(dbschema.HistoryTransactor)
	c.Assert(ok, qt.IsTrue)
	tx, err := owner.BeginHistoryTransaction(c.Context(), dbschema.HistoryIsolationSerializable)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = tx.Rollback() })
	c.Assert(readProviderHistory(c, tx).Records, qt.HasLen, 0)

	// Both sessions read an empty history and then insert different identities.
	// Both commits would be valid row comparisons, but cannot form a serial
	// execution: whichever transaction ran second would have seen the first row.
	second := connectTransactionAddress(c, host, address)
	other, err := second.HistoryStore()
	c.Assert(err, qt.IsNil)
	otherOwner, ok := other.(dbschema.HistoryTransactor)
	c.Assert(ok, qt.IsTrue)
	otherTx, err := otherOwner.BeginHistoryTransaction(c.Context(), dbschema.HistoryIsolationSerializable)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = otherTx.Rollback() })
	c.Assert(readProviderHistory(c, otherTx).Records, qt.HasLen, 0)
	concurrent := providerHistoryRecord("2")
	c.Assert(otherTx.CompareAndSwap(c.Context(), providerHistoryTable(), nil, &concurrent), qt.IsNil)
	c.Assert(otherTx.Commit(), qt.IsNil)
	c.Assert(readProviderHistory(c, tx).Records, qt.HasLen, 0)
	stale := providerHistoryRecord("1")
	c.Assert(tx.CompareAndSwap(c.Context(), providerHistoryTable(), nil, &stale), qt.IsNotNil)
	c.Assert(tx.Commit(), qt.ErrorMatches, "(?s)provider transaction cannot commit after failed execution: .*")
	c.Assert(tx.Rollback(), qt.IsNil)
	c.Assert(readProviderHistory(c, store).Records, qt.DeepEquals, []dbschema.HistoryRecord{concurrent})

	// A fresh snapshot can commit the same insertion after seeing the new row.
	fresh, err := owner.BeginHistoryTransaction(c.Context(), dbschema.HistoryIsolationSerializable)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = fresh.Rollback() })
	c.Assert(readProviderHistory(c, fresh).Records, qt.DeepEquals, []dbschema.HistoryRecord{concurrent})
	c.Assert(fresh.CompareAndSwap(c.Context(), providerHistoryTable(), nil, &stale), qt.IsNil)
	c.Assert(fresh.Commit(), qt.IsNil)
	c.Assert(readProviderHistory(c, other).Records, qt.DeepEquals, []dbschema.HistoryRecord{stale, concurrent})
}

func TestProviderHistoryIsolationRefusalsReleaseTheSession(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "history-transactions")
	conn, _ := transactionConnection(c, host, "")
	store, err := conn.HistoryStore()
	c.Assert(err, qt.IsNil)
	owner, ok := store.(dbschema.HistoryTransactor)
	c.Assert(ok, qt.IsTrue)
	_, err = owner.BeginHistoryTransaction(c.Context(), 0)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	_, err = owner.BeginHistoryTransaction(c.Context(), 99)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), "PRAGMA read_uncommitted=1;"), qt.IsNil)
	_, err = owner.BeginHistoryTransaction(c.Context(), dbschema.HistoryIsolationSerializable)
	c.Assert(err, qt.IsNotNil)
	// Default isolation remains available, but cannot satisfy a serializable
	// request. A refused begin must release its connection and fence its handle.
	tx, err := owner.BeginHistoryTransaction(c.Context(), dbschema.HistoryIsolationDefault)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = tx.Rollback() })
	c.Assert(tx.Rollback(), qt.IsNil)
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), "PRAGMA read_uncommitted=0;"), qt.IsNil)
	restored, err := owner.BeginHistoryTransaction(c.Context(), dbschema.HistoryIsolationSerializable)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = restored.Rollback() })
	c.Assert(restored.Rollback(), qt.IsNil)
}
