//go:build integration

package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// The tests in this file declare async replications and transfers as owned
// feature objects, which no source produces yet: the reader still reports
// both in the common catalog fields. ownedRead stands in for the reader the
// owner will have, so the comparison sees the objects in the form the owner
// compares.

// ownedRead reads the replication directory and moves the replications and
// transfers it reports into owned observations, with the namespace of each
// kind claimed in full: the test dropped everything else in the directory.
func ownedRead(c *qt.C, conn *dbschema.DatabaseConnection) *catalog.Database {
	c.Helper()
	live := readScoped(c, conn, replicationSchemas)
	complete := schemaext.Knowledge{State: schemaext.Complete}
	coverage := must.Must(live.FeatureCoverage.Combine(must.Must(ydbreplication.ReplicationCoverage(schemaext.Observed, complete, nil))))
	live.FeatureCoverage = must.Must(coverage.Combine(must.Must(ydbreplication.TransferCoverage(schemaext.Observed, complete, nil))))
	for _, replication := range live.AsyncReplications {
		live.FeatureObjects = must.Must(live.FeatureObjects.With(ydbreplication.ObservedReplicationObject(replication.Schema, replication.Name,
			replication.Spec, replication.State)))
	}
	for _, transfer := range live.Transfers {
		live.FeatureObjects = must.Must(live.FeatureObjects.With(ydbreplication.ObservedTransferObject(transfer.Schema, transfer.Name,
			transfer.Spec, transfer.State)))
	}
	live.AsyncReplications, live.Transfers = nil, nil
	return live
}

// ownedPlan plans the statements that take the replication directory, read
// by [ownedRead], to the declaration.
func ownedPlan(c *qt.C, conn *dbschema.DatabaseConnection, declared *schemamodel.Database) []string {
	c.Helper()
	info := conn.Info()
	runtime := must.Must(builtin.New())
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), declared, ownedRead(c, conn), info, nil, runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), runtime, diff, info.Dialect,
		planner.Options{Capabilities: info.Capabilities})
	c.Assert(err, qt.IsNil)
	return statements
}

// ownedDeclaration is declared with the replication and the transfer
// namespaces claimed in full and the owned objects given.
func ownedDeclaration(declared *schemamodel.Database, objects ...schemaext.Object) *schemamodel.Database {
	complete := schemaext.Knowledge{State: schemaext.Complete}
	for _, object := range objects {
		declared.FeatureObjects = must.Must(declared.FeatureObjects.With(object))
	}
	coverage := must.Must(declared.FeatureCoverage.Combine(must.Must(ydbreplication.ReplicationCoverage(schemaext.Desired, complete, nil))))
	declared.FeatureCoverage = must.Must(coverage.Combine(must.Must(ydbreplication.TransferCoverage(schemaext.Desired, complete, nil))))
	return declared
}

// TestYDBOwnedReplication_CreatesAndDropsWithItsReplica creates an owned
// replication of a table of the same database with the statement the common
// path writes, plans nothing once it runs, and drops it with CASCADE, which
// takes the replica table along, once the schema leaves it out.
func TestYDBOwnedReplication_CreatesAndDropsWithItsReplica(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropReplications(c, conn)
			c.Cleanup(func() { dropReplications(c, conn) })
			connection := selfConnection(c, conn)
			spec := replicationDeclaration(connection).AsyncReplications[0].Spec
			declared := ownedDeclaration(replicationDeclaration(""),
				ydbreplication.DesiredReplicationObject(replicationSchema, "mirror", "", spec))

			first := ownedPlan(c, conn, declared)
			c.Assert(first, qt.HasLen, 2)
			c.Assert(first[1], qt.Equals, "CREATE ASYNC REPLICATION `ptah_ydb_repl/mirror` FOR `/local/ptah_ydb_repl/src` "+
				"AS `ptah_ydb_repl/rep` WITH (CONNECTION_STRING = '"+connection+"')")
			apply(c, conn, first)
			live := settledRead(c, conn, "the replication running with its replica", replicaRecorded)
			c.Assert(ydbreplication.ReplicationsEqual(spec, live.AsyncReplications[0].Spec), qt.IsTrue)
			c.Assert(ownedPlan(c, conn, declared), qt.HasLen, 0)

			removed := ownedPlan(c, conn, ownedDeclaration(replicationDeclaration("")))
			c.Assert(removed, qt.DeepEquals, []string{"DROP ASYNC REPLICATION `ptah_ydb_repl/mirror` CASCADE"})
			apply(c, conn, removed)
			settledRead(c, conn, "the replication gone with its replica", func(live *catalog.Database) bool {
				return len(live.AsyncReplications) == 0 && len(live.NotDescribed.Objects) == 0
			})
			c.Assert(ownedPlan(c, conn, ownedDeclaration(replicationDeclaration(""))), qt.HasLen, 0)
		})
	}
}

// TestYDBOwnedTransfer_CreatesAfterItsTopicAndDropsBeforeIt creates a declared
// topic first and an owned transfer of it after the table it writes, plans
// nothing once the transfer runs, and drops the transfer before the topic when
// the schema leaves both out. YDB accepts DROP TOPIC under a running transfer,
// so only the plan's order keeps the topic from going first. The transfer
// reads through a consumer the topic declares: the consumer YDB would create
// for it otherwise is adopted from the common catalog fields, which
// [ownedRead] empties.
func TestYDBOwnedTransfer_CreatesAfterItsTopicAndDropsBeforeIt(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c, lineNamed(c, "26.2"))
	dropReplications(c, conn)
	c.Cleanup(func() { dropReplications(c, conn) })
	complete := schemaext.Knowledge{State: schemaext.Complete}
	withTopic := func(declared *schemamodel.Database) *schemamodel.Database {
		declared.FeatureCoverage = must.Must(declared.FeatureCoverage.Combine(must.Must(ydbtopic.Coverage(schemaext.Desired, complete, nil))))
		return declared
	}
	spec := ydbreplication.TransferSpec{Source: replicationSchema + "/events", Target: replicationSchema + "/order_log",
		Lambda: lambdaWriting("a:"), Consumer: "ingest", FlushInterval: "PT1S"}
	declared := withTopic(ownedDeclaration(transferDeclaration(""),
		ydbtopic.DesiredObject(replicationSchema, "events", "", ydbtopic.Spec{Consumers: []ydbtopic.ConsumerSpec{{Name: "ingest"}}}),
		ydbreplication.DesiredTransferObject(replicationSchema, "ingest", "", spec)))

	first := ownedPlan(c, conn, declared)
	c.Assert(first[0], qt.Equals, "CREATE TOPIC `ptah_ydb_repl/events` (CONSUMER `ingest`)")
	c.Assert(first[len(first)-1], qt.Equals, "CREATE TRANSFER `ptah_ydb_repl/ingest` FROM `ptah_ydb_repl/events` "+
		"TO `ptah_ydb_repl/order_log` USING "+lambdaWriting("a:")+" WITH (CONSUMER = 'ingest', FLUSH_INTERVAL = Interval('PT1S'))")
	apply(c, conn, first)
	settledRead(c, conn, "the transfer running", func(live *catalog.Database) bool {
		return len(live.Transfers) == 1 && live.Transfers[0].State == catalog.ReplicationRunning
	})
	c.Assert(ownedPlan(c, conn, declared), qt.HasLen, 0)

	removed := ownedPlan(c, conn, withTopic(ownedDeclaration(transferDeclaration(""))))
	c.Assert(removed, qt.DeepEquals, []string{"DROP TRANSFER `ptah_ydb_repl/ingest`", "DROP TOPIC `ptah_ydb_repl/events`"})
	apply(c, conn, removed)
	settledRead(c, conn, "the transfer gone", func(live *catalog.Database) bool { return len(live.Transfers) == 0 })
	c.Assert(ownedPlan(c, conn, withTopic(ownedDeclaration(transferDeclaration("")))), qt.HasLen, 0)
}
