//go:build integration

package ydb_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/sqlschema"
)

func desiredReplicationYQL(c *qt.C, source string) *schemamodel.Database {
	c.Helper()
	database, _, err := sqlschema.Read([]byte(source), "ydb")
	c.Assert(err, qt.IsNil)
	return &database
}

func TestYDBDesiredYQL_Replication(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropReplications(c, conn)
			c.Cleanup(func() { dropReplications(c, conn) })
			connection := selfConnection(c, conn)
			const table = "CREATE TABLE `ptah_ydb_repl/src` (id Int64 NOT NULL, note Utf8, PRIMARY KEY(id));"
			source := table + fmt.Sprintf("CREATE ASYNC REPLICATION `ptah_ydb_repl/mirror` FOR `/local/ptah_ydb_repl/src` AS `ptah_ydb_repl/rep` WITH (CONNECTION_STRING='%s');", connection)
			desired := desiredReplicationYQL(c, source)
			apply(c, conn, planAgainst(c, conn, desired, replicationSchemas))
			settledRead(c, conn, "the YQL replica recorded", replicaRecorded)
			c.Assert(planAgainst(c, conn, desired, replicationSchemas), qt.HasLen, 0)
			moved := alternateSelfConnection(c, connection)
			c.Assert(moved, qt.Not(qt.Equals), connection)
			changed := desiredReplicationYQL(c, source+fmt.Sprintf("ALTER ASYNC REPLICATION `ptah_ydb_repl/mirror` SET (CONNECTION_STRING='%s');", moved))
			c.Assert(planError(c, conn, changed), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			apply(c, conn, []string{"ALTER ASYNC REPLICATION `ptah_ydb_repl/mirror` SET (STATE='PAUSED')"})
			settledRead(c, conn, "the YQL replication paused", replicationState(ydbreplication.StatePaused))
			changes := planAgainst(c, conn, changed, replicationSchemas)
			c.Assert(changes, qt.HasLen, 1)
			apply(c, conn, changes)
			settledRead(c, conn, "the YQL connection updated", func(live *catalog.Database) bool {
				return replicationState(ydbreplication.StatePaused)(live) && liveReplications(live)[0].Spec.Connection.ConnectionString == moved
			})
			c.Assert(planAgainst(c, conn, changed, replicationSchemas), qt.HasLen, 0)
			omitted := desiredReplicationYQL(c, table)
			c.Assert(planAgainst(c, conn, omitted, replicationSchemas), qt.DeepEquals, []string{"DROP ASYNC REPLICATION `ptah_ydb_repl/mirror` CASCADE"})
			apply(c, conn, planAgainst(c, conn, omitted, replicationSchemas))
			settledRead(c, conn, "the omitted replication removed", func(live *catalog.Database) bool {
				return len(liveReplications(live)) == 0 && live.NotDescribed.Describes(ydbschema.CoverageReplicaTable, replicationSchema+".rep")
			})
			waitForDirectory(c, line, []string{"src"}, replicationSchema)
			c.Assert(planAgainst(c, conn, omitted, replicationSchemas), qt.HasLen, 0)
		})
	}
}

const yqlTransferObjects = "CREATE TOPIC `ptah_ydb_repl/events`; CREATE TABLE `ptah_ydb_repl/rows` (id Int64 NOT NULL, PRIMARY KEY(id));"
const yqlTransferInline = "($msg) -> { RETURN [<|id:Unwrap(CAST($msg._offset AS Int64))|>]; }"
const yqlTransferSource = yqlTransferObjects + "CREATE TRANSFER `ptah_ydb_repl/ingest` FROM `ptah_ydb_repl/events` TO `ptah_ydb_repl/rows` USING " + yqlTransferInline + " WITH (FLUSH_INTERVAL=Interval('PT1S'));"

func TestYDBDesiredYQL_Transfer(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c, lineNamed(c, "26.2"))
	dropReplications(c, conn)
	c.Cleanup(func() { dropReplications(c, conn) })
	desired := desiredReplicationYQL(c, yqlTransferSource)
	apply(c, conn, planAgainst(c, conn, desired, replicationSchemas))
	settledRead(c, conn, "the YQL transfer running", func(live *catalog.Database) bool {
		return len(liveTransfers(live)) == 1 && liveTransfers(live)[0].State == ydbreplication.StateRunning
	})
	c.Assert(planAgainst(c, conn, desired, replicationSchemas), qt.HasLen, 0)
	const lambda = "($msg) -> { RETURN [<|id:Unwrap(CAST($msg._offset AS Int64))+1l|>]; }"
	changed := desiredReplicationYQL(c, yqlTransferSource+"ALTER TRANSFER `ptah_ydb_repl/ingest` SET USING "+lambda+", SET (BATCH_SIZE_BYTES=4096,FLUSH_INTERVAL=Interval('PT2S'));")
	changes := planAgainst(c, conn, changed, replicationSchemas)
	c.Assert(changes, qt.HasLen, 1)
	apply(c, conn, changes)
	settledRead(c, conn, "the YQL transfer changed", func(live *catalog.Database) bool {
		return len(liveTransfers(live)) == 1 && liveTransfers(live)[0].Spec.Lambda == lambda && liveTransfers(live)[0].Spec.BatchSizeBytes == 4096
	})
	c.Assert(planAgainst(c, conn, changed, replicationSchemas), qt.HasLen, 0)
	dropDesiredYQLTransfer(c, conn)
}

func dropDesiredYQLTransfer(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	omitted := desiredReplicationYQL(c, yqlTransferObjects)
	changes := planAgainst(c, conn, omitted, replicationSchemas)
	c.Assert(changes, qt.DeepEquals, []string{"DROP TRANSFER `ptah_ydb_repl/ingest`"})
	apply(c, conn, changes)
	settledRead(c, conn, "the omitted YQL transfer removed", func(live *catalog.Database) bool { return len(liveTransfers(live)) == 0 })
	c.Assert(planAgainst(c, conn, omitted, replicationSchemas), qt.HasLen, 0)
}

func TestYDBDesiredYQL_TransferRefusedOn251(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c, lineNamed(c, "25.1"))
	dropReplications(c, conn)
	c.Cleanup(func() { dropReplications(c, conn) })
	c.Assert(planError(c, conn, desiredReplicationYQL(c, yqlTransferSource)), qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
}
