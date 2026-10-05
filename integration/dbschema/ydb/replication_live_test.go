//go:build integration

package ydb_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// replicationSchema is the directory the replication tests write into.
const replicationSchema = "ptah_ydb_repl"

var replicationSchemas = []string{replicationSchema}

// selfConnection is the connection string through which the server's own
// nodes reach its database: the address discovery advertises, which a
// replication of the database's own tables reads through.
func selfConnection(c *qt.C, conn *dbschema.DatabaseConnection) string {
	c.Helper()
	connector, ok := conn.SchemaWriter().(interface {
		SelfConnectionString(ctx context.Context) (string, error)
	})
	c.Assert(ok, qt.IsTrue, qt.Commentf("the YDB schema writer %T reads no cluster address", conn.SchemaWriter()))
	connection, err := connector.SelfConnectionString(c.Context())
	c.Assert(err, qt.IsNil)
	return connection
}

// replicationDeclaration declares the source table src in the replication
// directory, with the given extra tables, and a replication mirror of src into
// rep through connection, the same database.
func replicationDeclaration(connection string, tables ...string) *schemamodel.Database {
	db := &schemamodel.Database{}
	for _, name := range append([]string{"src"}, tables...) {
		db.Tables = append(db.Tables, schemamodel.Table{StructName: name, Name: name, Schema: replicationSchema})
		db.Fields = append(db.Fields,
			schemamodel.Field{StructName: name, Name: "id", Type: "BIGINT", Primary: true},
			schemamodel.Field{StructName: name, Name: "note", Type: "TEXT", Nullable: true})
	}
	if connection != "" {
		db.AsyncReplications = []schemamodel.AsyncReplication{{Name: "mirror", Schema: replicationSchema,
			Spec: ast.AsyncReplicationSpec{
				Connection: ast.ReplicationConnectionSpec{ConnectionString: connection},
				Items: []ast.AsyncReplicationItem{{Source: "/local/" + replicationSchema + "/src",
					Target: replicationSchema + "/rep"}},
			}}}
	}
	schemamodel.Finalize(db)
	return db
}

// dropReplications drops every transfer and replication in the replication
// directory, the replica tables with them, and then every table and topic.
func dropReplications(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	ctx := context.Background()
	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, replicationSchemas)
	c.Assert(err, qt.IsNil)
	for _, transfer := range live.Transfers {
		c.Assert(conn.Writer().ExecuteSQL(ctx, "DROP TRANSFER `"+replicationSchema+"/"+transfer.Name+"`"), qt.IsNil)
	}
	for _, replication := range live.AsyncReplications {
		c.Assert(conn.Writer().ExecuteSQL(ctx, "DROP ASYNC REPLICATION `"+replicationSchema+"/"+replication.Name+
			"` CASCADE"), qt.IsNil)
	}
	// CASCADE took the replica tables of the replications above; what is left
	// is a replica a replication dropped without CASCADE left behind.
	live, err = dbschema.ReadSchemaWithSchemasContext(ctx, conn, replicationSchemas)
	c.Assert(err, qt.IsNil)
	for _, object := range live.NotDescribed.Objects {
		if object.Kind == coverage.ReplicaTable {
			c.Assert(conn.Writer().ExecuteSQL(ctx, "DROP TABLE `"+strings.Replace(object.Name, ".", "/", 1)+"`"),
				qt.IsNil)
		}
	}
	for _, topic := range live.Topics {
		c.Assert(conn.Writer().ExecuteSQL(ctx, "DROP TOPIC `"+replicationSchema+"/"+topic.Name+"`"), qt.IsNil)
	}
	dropTables(c, conn, replicationSchemas)
}

// settledRead reads the replication directory until ready holds of the read,
// for up to a minute: YDB creates a replication's replica tables, moves its
// state, runs a transfer and drops replicas after the statement returns.
// Every read has to succeed: the reader waits out a table YDB lists and does
// not describe yet, which is the state such a change passes through, so a
// read that fails here is a reader defect rather than something to wait for.
func settledRead(c *qt.C, conn *dbschema.DatabaseConnection, what string,
	ready func(*catalog.Database) bool,
) *catalog.Database {
	c.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		live := readScoped(c, conn, replicationSchemas)
		if ready(live) {
			return live
		}
		if time.Now().After(deadline) {
			c.Fatalf("the read did not show %s within a minute: replications %+v, transfers %+v, not described %+v",
				what, live.AsyncReplications, live.Transfers, live.NotDescribed.Objects)
		}
		time.Sleep(time.Second)
	}
}

// replicaRecorded reports a read holding the running replication mirror with
// its item resolved and its replica table recorded rather than described.
func replicaRecorded(live *catalog.Database) bool {
	return len(live.AsyncReplications) == 1 && live.AsyncReplications[0].State == catalog.ReplicationRunning &&
		len(live.AsyncReplications[0].Spec.Items) == 1 &&
		!live.NotDescribed.Describes(coverage.ReplicaTable, replicationSchema+".rep")
}

// replicationState reports a read holding the replication mirror in state.
func replicationState(state string) func(*catalog.Database) bool {
	return func(live *catalog.Database) bool {
		return len(live.AsyncReplications) == 1 && live.AsyncReplications[0].State == state
	}
}

// planError plans the declaration against the replication directory and
// returns the refusal, the comparison's or the planner's: the comparison
// validates the declaration on its own, and the planner what the database
// holds besides.
func planError(c *qt.C, conn *dbschema.DatabaseConnection, declared *schemamodel.Database) error {
	c.Helper()
	info := conn.Info()
	diff, err := schemadiff.CompareWithDatabaseInfo(declared, readScoped(c, conn, replicationSchemas), info, nil)
	if err != nil {
		return err
	}
	_, err = planner.GenerateSchemaDiffSQLStatementsWithOptions(diff, info.Dialect,
		planner.Options{Capabilities: info.Capabilities})
	return err
}

// TestYDBReplication_RoundTrip creates a replication of a table of the same
// database, and plans nothing once it runs: the replica table YDB created is
// recorded rather than described, and so is the changefeed the replication
// added to its source, so neither reaches the plan and the schema declares
// neither. Removing the replication from the schema drops it with CASCADE,
// which takes the replica table, and plans nothing after.
func TestYDBReplication_RoundTrip(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropReplications(c, conn)
			c.Cleanup(func() { dropReplications(c, conn) })
			connection := selfConnection(c, conn)
			declared := replicationDeclaration(connection)

			first := planAgainst(c, conn, declared, replicationSchemas)
			c.Assert(first, qt.HasLen, 2)
			c.Assert(first[1], qt.Equals, "CREATE ASYNC REPLICATION `ptah_ydb_repl/mirror` FOR `/local/ptah_ydb_repl/src` "+
				"AS `ptah_ydb_repl/rep` WITH (CONNECTION_STRING = '"+connection+"')")
			apply(c, conn, first)

			live := settledRead(c, conn, "the replica recorded", replicaRecorded)
			c.Assert(live.AsyncReplications[0].Spec, qt.DeepEquals, ast.AsyncReplicationSpec{
				Connection: ast.ReplicationConnectionSpec{ConnectionString: connection},
				Items:      []ast.AsyncReplicationItem{{Source: "ptah_ydb_repl/src", Target: "ptah_ydb_repl/rep"}},
			})
			c.Assert(tableNames(live), qt.DeepEquals, []string{"ptah_ydb_repl|src"})
			c.Assert(planAgainst(c, conn, declared, replicationSchemas), qt.HasLen, 0)
			c.Assert(planAgainst(c, conn, declared, replicationSchemas), qt.HasLen, 0)

			removed := planAgainst(c, conn, replicationDeclaration(""), replicationSchemas)
			c.Assert(removed, qt.DeepEquals, []string{"DROP ASYNC REPLICATION `ptah_ydb_repl/mirror` CASCADE"})
			apply(c, conn, removed)
			settledRead(c, conn, "the replication and its replica gone", func(live *catalog.Database) bool {
				return len(live.AsyncReplications) == 0 &&
					live.NotDescribed.Describes(coverage.ReplicaTable, replicationSchema+".rep")
			})
			waitForDirectory(c, line, []string{"src"}, replicationSchema)
			c.Assert(planAgainst(c, conn, replicationDeclaration(""), replicationSchemas), qt.HasLen, 0)
		})
	}
}

// TestYDBReplication_RefusesATableAtAReplica refuses a schema that declares a
// table at a replica's path: beside the replication that creates it, since
// YDB creates the replica itself; without the replication, which a plain drop
// would leave read-only for good and CASCADE would drop; and where a
// replication dropped without CASCADE left a replica YDB keeps read-only
// (`path is an async replica table`).
func TestYDBReplication_RefusesATableAtAReplica(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropReplications(c, conn)
			c.Cleanup(func() { dropReplications(c, conn) })
			connection := selfConnection(c, conn)
			apply(c, conn, planAgainst(c, conn, replicationDeclaration(connection), replicationSchemas))
			settledRead(c, conn, "the replica recorded", replicaRecorded)

			kept := planError(c, conn, replicationDeclaration(connection, "rep"))
			dropped := planError(c, conn, replicationDeclaration("", "rep"))

			c.Assert(kept, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(kept, qt.ErrorMatches, `(?s).*table ptah_ydb_repl\.rep lies at a target of async replication `+
				`ptah_ydb_repl\.mirror, which creates its replica tables itself; declare the replication without the `+
				`table`)
			c.Assert(dropped, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(dropped, qt.ErrorMatches, `(?s).*async replication ptah_ydb_repl\.mirror: the schema drops it and `+
				`declares table ptah_ydb_repl\.rep, its replica, .* fail it over first .*`)

			apply(c, conn, []string{"DROP ASYNC REPLICATION `ptah_ydb_repl/mirror`"})
			orphaned := planError(c, conn, replicationDeclaration("", "rep"))
			c.Assert(orphaned, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(orphaned, qt.ErrorMatches, `(?s).*table ptah_ydb_repl\.rep: ptah_ydb_repl\.rep is a replica table `+
				`no replication of this database writes any more, which YDB keeps read-only for good .*`)
		})
	}
}

// TestYDBReplication_FailedOver fails a replication over by hand, after which
// YDB keeps its replica as an ordinary table: the read describes it, a plan
// that keeps the replication and leaves the table out is refused, and a
// schema that declares the table and removes the replication drops the
// replication without CASCADE and keeps the table, which takes writes.
func TestYDBReplication_FailedOver(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropReplications(c, conn)
			c.Cleanup(func() { dropReplications(c, conn) })
			connection := selfConnection(c, conn)
			apply(c, conn, planAgainst(c, conn, replicationDeclaration(connection), replicationSchemas))
			settledRead(c, conn, "the replica recorded", replicaRecorded)

			apply(c, conn, []string{"ALTER ASYNC REPLICATION `ptah_ydb_repl/mirror` SET (STATE = 'DONE', " +
				"FAILOVER_MODE = 'FORCE')"})
			live := settledRead(c, conn, "the replication failed over", replicationState(catalog.ReplicationDone))
			c.Assert(tableNames(live), qt.DeepEquals, []string{"ptah_ydb_repl|rep", "ptah_ydb_repl|src"})

			kept := planError(c, conn, replicationDeclaration(connection))
			c.Assert(kept, qt.ErrorMatches, `(?s).*table ptah_ydb_repl\.rep: it is a table async replication `+
				`ptah_ydb_repl\.mirror created and failed over, which the schema keeps; .*`)

			removed := planAgainst(c, conn, replicationDeclaration("", "rep"), replicationSchemas)
			c.Assert(removed, qt.DeepEquals, []string{"DROP ASYNC REPLICATION `ptah_ydb_repl/mirror`"})
			apply(c, conn, removed)
			apply(c, conn, []string{"UPSERT INTO `ptah_ydb_repl/rep` (id, note) VALUES (1l, 'written'u)"})
			c.Assert(scalar(c, conn, "SELECT COUNT(*) FROM `ptah_ydb_repl/rep`"), qt.Equals, int64(1))
			c.Assert(planAgainst(c, conn, replicationDeclaration("", "rep"), replicationSchemas), qt.HasLen, 0)
		})
	}
}

// TestYDBReplication_ConnectionChangesWhilePaused changes a replication's
// connection: refused while the replication runs, since YDB answers
// `Modifications are not allowed in StandBy state`, and planned as one ALTER
// once it is paused, after which the replication reads back with the new
// connection and the plan is empty. Ptah neither pauses nor resumes it.
func TestYDBReplication_ConnectionChangesWhilePaused(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropReplications(c, conn)
			c.Cleanup(func() { dropReplications(c, conn) })
			connection := selfConnection(c, conn)
			apply(c, conn, planAgainst(c, conn, replicationDeclaration(connection), replicationSchemas))
			settledRead(c, conn, "the replica recorded", replicaRecorded)
			// The same server through its loopback address: another
			// connection string that still reaches the database.
			moved := strings.Replace(connection, "localhost", "127.0.0.1", 1)
			c.Assert(moved, qt.Not(qt.Equals), connection)

			running := planError(c, conn, replicationDeclaration(moved))
			c.Assert(running, qt.ErrorMatches, `(?s).*async replication ptah_ydb_repl\.mirror: its connection or `+
				`credential differs, and YDB changes them only while the replication is paused .*it is running.*`)

			apply(c, conn, []string{"ALTER ASYNC REPLICATION `ptah_ydb_repl/mirror` SET (STATE = 'PAUSED')"})
			settledRead(c, conn, "the replication paused", replicationState(catalog.ReplicationPaused))
			paused := planAgainst(c, conn, replicationDeclaration(moved), replicationSchemas)
			c.Assert(paused, qt.DeepEquals, []string{"ALTER ASYNC REPLICATION `ptah_ydb_repl/mirror` SET " +
				"(CONNECTION_STRING = '" + moved + "')"})
			apply(c, conn, paused)

			settledRead(c, conn, "the paused replication on its new connection", func(live *catalog.Database) bool {
				return replicationState(catalog.ReplicationPaused)(live) &&
					live.AsyncReplications[0].Spec.Connection.ConnectionString == moved
			})
			c.Assert(planAgainst(c, conn, replicationDeclaration(moved), replicationSchemas), qt.HasLen, 0)
		})
	}
}

// transferDeclaration declares the table orders with the changefeed feed, the
// table order_log a transfer writes, and the transfer ingest from the
// changefeed's topic through lambda, or no transfer for an empty lambda.
func transferDeclaration(lambda string) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Orders", Name: "orders", Schema: replicationSchema,
				Changefeeds: []ast.ChangefeedSpec{{Name: "feed", Mode: "NEW_IMAGE", Format: "JSON"}}},
			{StructName: "Log", Name: "order_log", Schema: replicationSchema},
		},
		Fields: []schemamodel.Field{
			{StructName: "Orders", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Log", Name: "partition", Type: "INT UNSIGNED", Primary: true},
			{StructName: "Log", Name: "offset", Type: "BIGINT UNSIGNED", Primary: true},
			{StructName: "Log", Name: "message", Type: "TEXT", Nullable: true},
		},
	}
	if lambda != "" {
		db.Transfers = []schemamodel.Transfer{{Name: "ingest", Schema: replicationSchema, Spec: ast.TransferSpec{
			Source: replicationSchema + "/orders/feed", Target: replicationSchema + "/order_log", Lambda: lambda,
			FlushInterval: "PT1S",
		}}}
	}
	schemamodel.Finalize(db)
	return db
}

// lambdaWriting is a transfer lambda that writes each message's position and
// the text it carries, with suffix written in front of the text.
func lambdaWriting(suffix string) string {
	return fmt.Sprintf("($msg) -> { return [<| partition: $msg._partition, offset: $msg._offset, "+
		"message: '%s'u || CAST($msg._data AS Utf8) |>]; }", suffix)
}

// TestYDBTransfer_RoundTrip creates a transfer from a declared changefeed's
// topic into a declared table, after both, and plans nothing once it runs:
// the consumer YDB created for it on the changefeed's topic stays. A row
// written to the table reaches the transfer's table. A changed lambda is one
// ALTER, and removing the transfer drops it before anything else.
func TestYDBTransfer_RoundTrip(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c, lineNamed(c, "26.2"))
	dropReplications(c, conn)
	c.Cleanup(func() { dropReplications(c, conn) })

	runTransfer(c, conn, transferDeclaration(lambdaWriting("a:")))
}

// TestYDBTransfer_RefusedOn251 refuses a transfer on 25.1, which creates none
// with its default flags (`Topic transfer creation is disabled`), before any
// statement runs.
func TestYDBTransfer_RefusedOn251(t *testing.T) {
	c := qt.New(t)
	line := lineNamed(c, "25.1")
	conn := openYDB(c, line)
	dropReplications(c, conn)
	c.Cleanup(func() { dropReplications(c, conn) })

	err := planError(c, conn, transferDeclaration(lambdaWriting("a:")))

	c.Assert(line.preset().Has(capability.Transfers), qt.IsFalse)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*transfer ptah_ydb_repl\.ingest, which requires target capability `+
		`transfers, unavailable on this ydb target.*`)
}

// runTransfer applies the transfer, moves a row through it, changes its
// lambda and drops it.
func runTransfer(c *qt.C, conn *dbschema.DatabaseConnection, declared *schemamodel.Database) {
	c.Helper()
	first := planAgainst(c, conn, declared, replicationSchemas)
	c.Assert(first[len(first)-1], qt.Equals, "CREATE TRANSFER `ptah_ydb_repl/ingest` FROM `ptah_ydb_repl/orders/feed` "+
		"TO `ptah_ydb_repl/order_log` USING "+lambdaWriting("a:")+" WITH (FLUSH_INTERVAL = Interval('PT1S'))")
	apply(c, conn, first)
	apply(c, conn, []string{"UPSERT INTO `ptah_ydb_repl/orders` (id) VALUES (1l)"})
	live := settledRead(c, conn, "the transfer running", func(live *catalog.Database) bool {
		return len(live.Transfers) == 1 && live.Transfers[0].State == catalog.ReplicationRunning
	})
	c.Assert(live.Transfers[0].Spec.Consumer, qt.Not(qt.Equals), "")
	c.Assert(planAgainst(c, conn, declared, replicationSchemas), qt.HasLen, 0)
	waitForRows(c, conn, "SELECT COUNT(*) FROM `ptah_ydb_repl/order_log` WHERE StartsWith(message, 'a:')", 1)

	relambda := transferDeclaration(lambdaWriting("b:"))
	changed := planAgainst(c, conn, relambda, replicationSchemas)
	c.Assert(changed, qt.DeepEquals, []string{"ALTER TRANSFER `ptah_ydb_repl/ingest` SET USING " + lambdaWriting("b:")})
	apply(c, conn, changed)
	c.Assert(planAgainst(c, conn, relambda, replicationSchemas), qt.HasLen, 0)
	apply(c, conn, []string{"UPSERT INTO `ptah_ydb_repl/orders` (id) VALUES (2l)"})
	waitForRows(c, conn, "SELECT COUNT(*) FROM `ptah_ydb_repl/order_log` WHERE StartsWith(message, 'b:')", 1)

	removed := planAgainst(c, conn, transferDeclaration(""), replicationSchemas)
	c.Assert(removed, qt.DeepEquals, []string{"DROP TRANSFER `ptah_ydb_repl/ingest`"})
	apply(c, conn, removed)
	settledRead(c, conn, "the transfer gone", func(live *catalog.Database) bool { return len(live.Transfers) == 0 })
	c.Assert(planAgainst(c, conn, transferDeclaration(""), replicationSchemas), qt.HasLen, 0)
}

// waitForRows reads count until it answers want, for up to a minute: a
// transfer writes on its flush interval.
func waitForRows(c *qt.C, conn *dbschema.DatabaseConnection, count string, want int64) {
	c.Helper()
	deadline := time.Now().Add(time.Minute)
	for scalar(c, conn, count) != want {
		if time.Now().After(deadline) {
			c.Fatalf("%s did not answer %d within a minute", count, want)
		}
		time.Sleep(time.Second)
	}
}

// TestYDBTransfer_FromATopic creates a transfer of a declared standalone topic
// after the topic, and plans nothing once it runs: the consumer YDB created
// for it stays on the topic the schema declares without it.
func TestYDBTransfer_FromATopic(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c, lineNamed(c, "26.2"))
	dropReplications(c, conn)
	c.Cleanup(func() { dropReplications(c, conn) })
	declared := transferDeclaration("")
	declared.Topics = []schemamodel.Topic{{Name: "events", Schema: replicationSchema}}
	declared.Transfers = []schemamodel.Transfer{{Name: "ingest", Schema: replicationSchema, Spec: ast.TransferSpec{
		Source: replicationSchema + "/events", Target: replicationSchema + "/order_log", Lambda: lambdaWriting("t:"),
	}}}

	first := planAgainst(c, conn, declared, replicationSchemas)
	c.Assert(first[len(first)-2:], qt.DeepEquals, []string{
		"CREATE TOPIC `ptah_ydb_repl/events`",
		"CREATE TRANSFER `ptah_ydb_repl/ingest` FROM `ptah_ydb_repl/events` TO `ptah_ydb_repl/order_log` USING " +
			lambdaWriting("t:"),
	})
	apply(c, conn, first)
	settledRead(c, conn, "the transfer's consumer on the topic", func(live *catalog.Database) bool {
		return len(live.Topics) == 1 && len(live.Topics[0].Spec.Consumers) == 1
	})
	c.Assert(planAgainst(c, conn, declared, replicationSchemas), qt.HasLen, 0)
}

// TestYDBWriter_DropAllTablesDropsReplicationsFirst drops every transfer and
// replication before the tables: a running replication with CASCADE, which
// takes its replica table, and leaves the replica a replication dropped
// earlier without CASCADE left behind, which the reader records rather than
// describes.
func TestYDBWriter_DropAllTablesDropsReplicationsFirst(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropReplications(c, conn)
			c.Cleanup(func() { dropReplications(c, conn) })
			connection := selfConnection(c, conn)
			apply(c, conn, planAgainst(c, conn, replicationDeclaration(connection), replicationSchemas))
			settledRead(c, conn, "the replica recorded", replicaRecorded)
			apply(c, conn, []string{
				"CREATE ASYNC REPLICATION `ptah_ydb_repl/orphaning` FOR `/local/ptah_ydb_repl/src` AS " +
					"`ptah_ydb_repl/orphan` WITH (CONNECTION_STRING = '" + connection + "')",
			})
			settledRead(c, conn, "the second replica recorded", func(live *catalog.Database) bool {
				return !live.NotDescribed.Describes(coverage.ReplicaTable, replicationSchema+".orphan")
			})
			apply(c, conn, []string{"DROP ASYNC REPLICATION `ptah_ydb_repl/orphaning`"})

			c.Assert(conn.SchemaWriter().DropAllTables(c.Context()), qt.IsNil)

			settledRead(c, conn, "only the orphaned replica left", func(live *catalog.Database) bool {
				return len(live.AsyncReplications) == 0 && len(live.Tables) == 0 &&
					!live.NotDescribed.Describes(coverage.ReplicaTable, replicationSchema+".orphan") &&
					live.NotDescribed.Describes(coverage.ReplicaTable, replicationSchema+".rep")
			})
			waitForDirectory(c, line, []string{"orphan"}, replicationSchema)
		})
	}
}

// TestYDBWriter_DropDirectoryDropsReplicationsFirst tears down a directory
// holding a running replication and its replica table: the replication goes
// first, without CASCADE, and the teardown's own DROP TABLE takes the replica,
// which YDB accepts for a table it keeps read-only.
func TestYDBWriter_DropDirectoryDropsReplicationsFirst(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropper, ok := conn.SchemaWriter().(interface {
				DropDirectory(ctx context.Context, dir string) error
			})
			c.Assert(ok, qt.IsTrue, qt.Commentf("the YDB schema writer %T removes no directory", conn.SchemaWriter()))
			dropReplications(c, conn)
			c.Cleanup(func() {
				dropProbeDirectory(c, line, dropper)
				dropReplications(c, conn)
			})
			connection := selfConnection(c, conn)
			apply(c, conn, []string{
				"CREATE TABLE `ptah_ydb_repl/src` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))",
				"CREATE ASYNC REPLICATION `ptah_ydb_repl/probe/mirror` FOR `/local/ptah_ydb_repl/src` AS " +
					"`ptah_ydb_repl/probe/rep` WITH (CONNECTION_STRING = '" + connection + "')",
			})
			waitForEntry(c, line, "rep", replicationSchema, "probe")

			c.Assert(dropper.DropDirectory(c.Context(), "ptah_ydb_repl/probe"), qt.IsNil)

			c.Assert(directoryNames(c, c.Context(), line, replicationSchema), qt.DeepEquals, []string{"src"})
		})
	}
}

// waitForEntry lists the directory named by segments until it holds name, for
// up to a minute: YDB creates a replica table after CREATE ASYNC REPLICATION
// returns.
func waitForEntry(c *qt.C, line ydbLine, name string, segments ...string) {
	c.Helper()
	deadline := time.Now().Add(time.Minute)
	for !slices.Contains(directoryNames(c, c.Context(), line, segments...), name) {
		if time.Now().After(deadline) {
			c.Fatalf("%s did not appear in %v within a minute", name, segments)
		}
		time.Sleep(time.Second)
	}
}

// dropProbeDirectory removes the directory a failed teardown test left behind,
// and nothing when the test removed it.
func dropProbeDirectory(c *qt.C, line ydbLine, dropper interface {
	DropDirectory(ctx context.Context, dir string) error
},
) {
	c.Helper()
	if slices.Contains(directoryNames(c, context.Background(), line, replicationSchema), "probe") {
		c.Check(dropper.DropDirectory(context.Background(), replicationSchema+"/probe"), qt.IsNil)
	}
}

// waitForDirectory lists the directory named by segments until it holds want,
// for up to a minute: a listing follows a drop a moment after the drop
// returns.
func waitForDirectory(c *qt.C, line ydbLine, want []string, segments ...string) {
	c.Helper()
	deadline := time.Now().Add(time.Minute)
	for !slices.Equal(directoryNames(c, c.Context(), line, segments...), want) {
		if time.Now().After(deadline) {
			c.Fatalf("%v did not come to hold %v within a minute: it holds %v", segments, want,
				directoryNames(c, c.Context(), line, segments...))
		}
		time.Sleep(time.Second)
	}
}

// TestYDBReplication_ReadsWhileReplicasComeAndGo reads the directory straight
// after a replication is created and straight after it is dropped with
// CASCADE, five times on each line. The replica is then being created or
// dropped, and YDB may list it while it does not describe it: GitHub's runners
// met that state on 25.1.4.7. Every read succeeds, and records the replica
// only once YDB describes it.
func TestYDBReplication_ReadsWhileReplicasComeAndGo(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			dropReplications(c, conn)
			c.Cleanup(func() { dropReplications(c, conn) })
			connection := selfConnection(c, conn)
			apply(c, conn, []string{"CREATE TABLE `ptah_ydb_repl/src` (`id` Int64 NOT NULL, PRIMARY KEY (`id`))"})

			for range 5 {
				apply(c, conn, []string{"CREATE ASYNC REPLICATION `ptah_ydb_repl/cycle` FOR " +
					"`/local/ptah_ydb_repl/src` AS `ptah_ydb_repl/cycle_rep` WITH (CONNECTION_STRING = '" +
					connection + "')"})
				created := readScoped(c, conn, replicationSchemas)
				apply(c, conn, []string{"DROP ASYNC REPLICATION `ptah_ydb_repl/cycle` CASCADE"})
				dropped := readScoped(c, conn, replicationSchemas)

				c.Assert(created.AsyncReplications, qt.HasLen, 1)
				c.Assert(tableNames(created), qt.DeepEquals, []string{"ptah_ydb_repl|src"})
				c.Assert(dropped.AsyncReplications, qt.HasLen, 0)
				c.Assert(dropped.NotDescribed.Describes(coverage.ReplicaTable, replicationSchema+".cycle_rep"),
					qt.IsTrue)
			}
		})
	}
}
