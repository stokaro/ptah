package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// primary is a connection to the database a replication reads.
var primary = ast.ReplicationConnectionSpec{ConnectionString: "grpc://primary:2136/?database=/prod"}

// replicationOf is a replication of /prod's table source into target.
func replicationOf(source, target string) ast.AsyncReplicationSpec {
	return ast.AsyncReplicationSpec{Connection: primary,
		Items: []ast.AsyncReplicationItem{{Source: source, Target: target}}}
}

// heldReplication is a replication the database holds, in state.
func heldReplication(name, target, state string) catalog.AsyncReplication {
	return catalog.AsyncReplication{Name: name, State: state, Spec: replicationOf(name, target)}
}

// table is a declared table with a key.
func table(name string) schemamodel.Table {
	return schemamodel.Table{StructName: "S", Name: name}
}

// lambda is the lambda every transfer below writes rows with.
const lambda = "($m) -> { return []; }"

// replicaOf is the read's record of the replica table at name.
func replicaOf(name string) coverage.Object {
	return coverage.Object{Kind: coverage.ReplicaTable, Name: name, Reason: coverage.Unsupported,
		Provenance: coverage.Observed}
}

// TestGenerateMigrationAST_Replications_HappyPath pins where replications and
// transfers go in a YDB plan: every transfer and then every replication the
// plan removes goes first, a running one with CASCADE and a failed-over one
// without; every one the plan creates or changes comes after the tables are
// created and dropped, the replications before the transfers, and before the
// views.
func TestGenerateMigrationAST_Replications_HappyPath(t *testing.T) {
	c := qt.New(t)
	feed := ydbschema.ChangefeedSpec{Name: "feed", Mode: "NEW_IMAGE", Format: "JSON"}
	moved := replicationOf("paused", "paused_copy")
	moved.Connection.ConnectionString = "grpc://standby:2136/?database=/prod"
	ingest := ast.TransferSpec{Source: "orders/feed", Target: "order_log", Lambda: lambda}
	relambda := ast.TransferSpec{Source: "orders/feed", Target: "order_log", Lambda: "($m) -> { return [1]; }"}
	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableChanges{{Name: "orders", Table: table("orders"),
			OwnedObjects: declaredFeeds(t, schemacapture.TableDeclaration{Table: table("orders")}, feed).OwnedObjects,
			Fields:       []schemamodel.Field{keyField("id")}}},
		TablesRemoved: difftypes.TableRemovals{{Name: "legacy"}},
		AsyncReplicationsAdded: difftypes.AsyncReplicationChanges{
			{Name: "mirror", Spec: replicationOf("accounts", "replica/accounts")},
		},
		AsyncReplicationsRemoved: difftypes.AsyncReplicationChanges{
			{Name: "failed_over", Spec: replicationOf("failed_over", "failed_over_copy")},
			{Name: "running", Spec: replicationOf("running", "running_copy")},
		},
		AsyncReplicationsModified: []difftypes.AsyncReplicationDiff{{Name: "paused", ConnectionChanged: true,
			Desired: moved, Current: replicationOf("paused", "paused_copy"), State: catalog.ReplicationPaused}},
		TransfersAdded:   difftypes.TransferChanges{{Name: "ingest", Spec: ingest}},
		TransfersRemoved: difftypes.TransferChanges{{Name: "old_ingest", Spec: ingest}},
		TransfersModified: []difftypes.TransferDiff{{Name: "reingest", LambdaChanged: true, Desired: relambda,
			Current: ingest, State: catalog.ReplicationRunning}},
		ViewsAdded: difftypes.ViewChanges{{Name: "v", Body: "SELECT id FROM orders"}},
		DeclaredTables: []schemamodel.Table{
			table("orders"), table("order_log"),
		},
		Replications: difftypes.ReplicationContext{
			DesiredObjects: declaredFeeds(t, schemacapture.TableDeclaration{Table: table("orders")}, feed).OwnedObjects,
			CurrentReplications: []catalog.AsyncReplication{
				heldReplication("failed_over", "failed_over_copy", catalog.ReplicationDone),
				heldReplication("running", "running_copy", catalog.ReplicationRunning),
				heldReplication("paused", "paused_copy", catalog.ReplicationPaused),
			},
			DeclaredTransfers: []schemamodel.Transfer{{Name: "ingest", Spec: ingest},
				{Name: "reingest", Spec: relambda}},
		},
	}

	got := render(c, capability.YDB262(), diff)

	c.Assert(got, qt.Equals, "DROP TRANSFER `old_ingest`;\n"+
		"DROP ASYNC REPLICATION `failed_over`;\n"+
		"DROP ASYNC REPLICATION `running` CASCADE;\n"+
		"CREATE TABLE `orders` (\n"+
		"    `id` Int64 NOT NULL,\n"+
		"    PRIMARY KEY (`id`)\n"+
		");\n"+
		"ALTER TABLE `orders` ADD CHANGEFEED `feed` WITH (MODE = 'NEW_IMAGE', FORMAT = 'JSON');\n"+
		"DROP TABLE `legacy`;\n"+
		"CREATE ASYNC REPLICATION `mirror` FOR `accounts` AS `replica/accounts` WITH ("+
		"CONNECTION_STRING = 'grpc://primary:2136/?database=/prod');\n"+
		"ALTER ASYNC REPLICATION `paused` SET (CONNECTION_STRING = 'grpc://standby:2136/?database=/prod');\n"+
		"CREATE TRANSFER `ingest` FROM `orders/feed` TO `order_log` USING ($m) -> { return []; };\n"+
		"ALTER TRANSFER `reingest` SET USING ($m) -> { return [1]; };\n"+
		"CREATE VIEW `v` WITH (security_invoker = TRUE) AS\n"+
		"SELECT id FROM orders\n"+
		";\n")
}

// TestGenerateMigrationAST_Replications_ProposeNothingForReplicaTables plans
// nothing over a database holding a running replication declared as it is:
// the replica table is recorded rather than described, so neither the table
// nor the changefeed the replication added to its source reaches the plan,
// and the schema need not declare either.
func TestGenerateMigrationAST_Replications_ProposeNothingForReplicaTables(t *testing.T) {
	c := qt.New(t)
	spec := replicationOf("/local/src", "rep")
	spec.Connection.ConnectionString = "grpc://localhost:2136/?database=/local"
	declared := &schemamodel.Database{
		Tables: []schemamodel.Table{table("src")},
		Fields: []schemamodel.Field{keyField("id")},
		AsyncReplications: []schemamodel.AsyncReplication{
			{Name: "mirror", Spec: spec},
		},
	}
	read := &catalog.Database{
		Tables: []catalog.Table{{Name: "src", Type: "TABLE", Columns: []catalog.Column{{Name: "id",
			DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1}}}},
		Constraints: []catalog.Constraint{{Name: "src_pkey", TableName: "src", Type: "PRIMARY KEY",
			ColumnName: "id", ColumnNames: []string{"id"}}},
		AsyncReplications: []catalog.AsyncReplication{{Name: "mirror", State: catalog.ReplicationRunning,
			Spec: ast.AsyncReplicationSpec{Connection: spec.Connection,
				Items: []ast.AsyncReplicationItem{{Source: "src", Target: "rep"}}}}},
		NotDescribed: coverage.Set{}.With(replicaOf("rep")),
	}
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	declared.FeatureCoverage = feedCoverage(t, schemaext.Desired)
	read.FeatureCoverage = feedCoverage(t, schemaext.Observed, schemaext.SubjectCoverage{
		Kind: ydbschema.ChangefeedKind, Subject: ydbschema.ChangefeedRef("", "src", "2b1f0c5e-0d1c-4b1a-9c3e-5d6f7a8b9c0d"),
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "replication stream"}})
	diff, diagnostics, err := schemadiff.CompareReportingUndecidedAdditions(t.Context(), declared, read, &config.CompareOptions{Dialect: platform.YDB}, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diagnostics.Features, qt.HasLen, 1)

	nodes, err := ydb.NewWithCapabilities(capability.YDB262()).GenerateMigrationAST(
		context.Background(), must.Must(builtin.New()),
		diff,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 0)
}

// TestGenerateMigrationAST_Replications_TransferReadsARecordedTopic plans a
// transfer of a topic the read recorded rather than described, a topic of
// its own or a changefeed's: both are there for the transfer to read.
func TestGenerateMigrationAST_Replications_TransferReadsARecordedTopic(t *testing.T) {
	tests := []struct {
		name            string
		source          string
		limits          coverage.Set
		tables          []schemamodel.Table
		featureCoverage schemaext.Coverage
	}{
		{name: "a topic", source: "events", limits: coverage.Set{}.With(coverage.Object{Kind: coverage.Topic, Name: "events"}), tables: []schemamodel.Table{table("order_log")}},
		{name: "a changefeed in a directory", source: "app/orders/feed", tables: []schemamodel.Table{table("order_log"), {Schema: "app", Name: "orders"}},
			featureCoverage: feedCoverage(t, schemaext.Observed, schemaext.SubjectCoverage{Kind: ydbschema.ChangefeedKind, Subject: ydbschema.ChangefeedRef("app", "orders", "feed"), Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unsupported stream"}})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := ast.TransferSpec{Source: test.source, Target: "order_log", Lambda: lambda}
			diff := &difftypes.SchemaDiff{
				TransfersAdded:      difftypes.TransferChanges{{Name: "ingest", Spec: spec}},
				DeclaredTables:      test.tables,
				CurrentNotDescribed: test.limits,
				Replications: difftypes.ReplicationContext{
					CurrentCoverage:   test.featureCoverage,
					DeclaredTransfers: []schemamodel.Transfer{{Name: "ingest", Spec: spec}},
				},
			}

			got := render(c, capability.YDB262(), diff)

			c.Assert(got, qt.Equals, "CREATE TRANSFER `ingest` FROM `"+test.source+
				"` TO `order_log` USING ($m) -> { return []; };\n")
		})
	}
}

// TestGenerateMigrationAST_Replications_TransferReadsADeclaredTopic plans a
// transfer of a topic the schema declares after the topic is created, and
// refuses one whose topic the plan drops while the schema keeps the transfer.
func TestGenerateMigrationAST_Replications_TransferReadsADeclaredTopic(t *testing.T) {
	spec := ast.TransferSpec{Source: "app/events", Target: "order_log", Lambda: lambda}
	transfer := schemamodel.Transfer{Name: "ingest", Spec: spec}
	t.Run("created after its topic", func(t *testing.T) {
		c := qt.New(t)
		diff := &difftypes.SchemaDiff{
			TopicsAdded:    difftypes.TopicChanges{{Name: "events", Schema: "app"}},
			TransfersAdded: difftypes.TransferChanges{transfer},
			DeclaredTables: []schemamodel.Table{table("order_log")},
			Replications: difftypes.ReplicationContext{
				DeclaredTransfers: []schemamodel.Transfer{transfer},
				DeclaredTopics:    []string{"app/events"},
			},
		}

		got := render(c, capability.YDB262(), diff)

		c.Assert(got, qt.Equals, "CREATE TOPIC `app/events`;\n"+
			"CREATE TRANSFER `ingest` FROM `app/events` TO `order_log` USING ($m) -> { return []; };\n")
	})
	t.Run("its topic dropped", func(t *testing.T) {
		c := qt.New(t)
		diff := &difftypes.SchemaDiff{
			TopicsRemoved:  difftypes.TopicChanges{{Name: "events", Schema: "app"}},
			DeclaredTables: []schemamodel.Table{table("order_log")},
			Replications: difftypes.ReplicationContext{
				CurrentTransfers:  []catalog.Transfer{{Name: "ingest", Spec: spec}},
				DeclaredTransfers: []schemamodel.Transfer{transfer},
				CurrentTopics:     []string{"app/events"},
			},
		}

		nodes, err := ydb.NewWithCapabilities(capability.YDB262()).GenerateMigrationAST(
			context.Background(), must.Must(builtin.New()),
			diff,
		)

		c.Assert(err, qt.ErrorMatches, `transfer ingest: it reads topic app/events, which the schema declares `+
			`neither as a topic nor as a changefeed, .*`)
		c.Assert(nodes, qt.IsNil)
	})
}

// TestGenerateMigrationAST_Replications_TransferDroppedBeforeItsTopic drops a
// transfer before the topic it reads, when the plan removes both.
func TestGenerateMigrationAST_Replications_TransferDroppedBeforeItsTopic(t *testing.T) {
	c := qt.New(t)
	spec := ast.TransferSpec{Source: "events", Target: "order_log", Lambda: lambda}
	diff := &difftypes.SchemaDiff{
		TopicsRemoved:    difftypes.TopicChanges{{Name: "events"}},
		TransfersRemoved: difftypes.TransferChanges{{Name: "ingest", Spec: spec}},
		Replications: difftypes.ReplicationContext{
			CurrentTransfers: []catalog.Transfer{{Name: "ingest", Spec: spec}},
			CurrentTopics:    []string{"events"},
		},
	}

	got := render(c, capability.YDB262(), diff)

	c.Assert(got, qt.Equals, "DROP TRANSFER `ingest`;\nDROP TOPIC `events`;\n")
}

// TestGenerateMigrationAST_Replications_FailurePath refuses, before any
// statement, a plan YDB cannot run or one that would break what a replication
// or a transfer owns or depends on.
func TestGenerateMigrationAST_Replications_FailurePath(t *testing.T) {
	ingest := ast.TransferSpec{Source: "orders/feed", Target: "order_log", Lambda: lambda}
	feed := ydbschema.ChangefeedSpec{Name: "feed", Mode: "UPDATES", Format: "JSON"}
	streams := declaredFeeds(t, schemacapture.TableDeclaration{Table: table("orders")}, feed).OwnedObjects
	withTransfer := func(spec ast.TransferSpec, objects schemaext.Objects, tables ...schemamodel.Table) *difftypes.SchemaDiff {
		return &difftypes.SchemaDiff{
			TransfersAdded: difftypes.TransferChanges{{Name: "ingest", Spec: spec}},
			DeclaredTables: tables,
			Replications: difftypes.ReplicationContext{
				DesiredObjects:    objects,
				DeclaredTransfers: []schemamodel.Transfer{{Name: "ingest", Spec: spec}},
			},
		}
	}
	global := replicationOf("a", "ra")
	global.ConsistencyLevel = "global"
	moved := replicationOf("a", "ra")
	moved.Connection.ConnectionString = "grpc://standby:2136/?database=/prod"
	retargeted := ingest
	retargeted.Target = "other_log"
	tests := []struct {
		name    string
		caps    capability.Capabilities
		diff    *difftypes.SchemaDiff
		wantErr string
	}{
		{
			name:    "a transfer on 25.1",
			caps:    capability.YDB251(),
			diff:    withTransfer(ingest, streams, table("orders"), table("order_log")),
			wantErr: `transfer ingest, which requires target capability transfers, unavailable on this ydb target`,
		},
		{
			name: "a replication's level changed",
			caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{AsyncReplicationsModified: []difftypes.AsyncReplicationDiff{{Name: "mirror",
				CreateOnlyChanged: []string{"consistency_level"}, Desired: global, Current: replicationOf("a", "ra"),
				State: catalog.ReplicationPaused}}},
			wantErr: `async replication mirror: its consistency_level differ from the database's, .*`,
		},
		{
			name: "a running replication's connection changed",
			caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{AsyncReplicationsModified: []difftypes.AsyncReplicationDiff{{Name: "mirror",
				ConnectionChanged: true, Desired: moved, Current: replicationOf("a", "ra"),
				State: catalog.ReplicationRunning}}},
			wantErr: `async replication mirror: its connection or credential differs, and YDB changes them only while ` +
				`the replication is paused .*`,
		},
		{
			name: "a running replication dropped while the schema declares its replica",
			caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{
				AsyncReplicationsRemoved: difftypes.AsyncReplicationChanges{{Name: "mirror", Spec: replicationOf("a", "ra")}},
				DeclaredTables:           []schemamodel.Table{table("ra")},
				Replications: difftypes.ReplicationContext{CurrentReplications: []catalog.AsyncReplication{
					heldReplication("mirror", "ra", catalog.ReplicationRunning)}},
			},
			wantErr: "async replication mirror: the schema drops it and declares table ra, its replica, .*" +
				"fail it over first with ALTER ASYNC REPLICATION `mirror` SET \\(STATE = 'DONE', " +
				"FAILOVER_MODE = 'FORCE'\\), which makes its tables ordinary, and plan again",
		},
		{
			name: "a table declared at a running replica",
			caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{
				TablesAdded: difftypes.TableChanges{{Name: "ra", Table: table("ra"),
					Fields: []schemamodel.Field{keyField("id")}}},
				CurrentNotDescribed: coverage.Set{}.With(replicaOf("ra")),
				Replications: difftypes.ReplicationContext{CurrentReplications: []catalog.AsyncReplication{
					heldReplication("mirror", "ra", catalog.ReplicationRunning)}},
			},
			wantErr: `table ra: it is a replica table async replication mirror writes, read-only while the ` +
				`replication runs .*`,
		},
		{
			name: "a table declared at an orphaned replica",
			caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{
				TablesAdded: difftypes.TableChanges{{Name: "app.ra", Table: schemamodel.Table{StructName: "S",
					Name: "ra", Schema: "app"}, Fields: []schemamodel.Field{keyField("id")}}},
				CurrentNotDescribed: coverage.Set{}.With(replicaOf("app.ra")),
			},
			wantErr: `table app.ra: app.ra is a replica table no replication of this database writes any more, ` +
				`which YDB keeps read-only for good .*`,
		},
		{
			name: "a replication created over another's replica",
			caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{
				AsyncReplicationsAdded: difftypes.AsyncReplicationChanges{{Name: "second",
					Spec: replicationOf("b", "replica")}},
				CurrentNotDescribed: coverage.Set{}.With(replicaOf("replica.accounts")),
			},
			wantErr: `async replication second: its target replica holds replica/accounts, a replica table of ` +
				`another replication, .*`,
		},
		{
			name: "a failed-over replica dropped while the schema keeps its replication",
			caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{
				TablesRemoved: difftypes.TableRemovals{{Name: "ra"}},
				Replications: difftypes.ReplicationContext{
					CurrentReplications: []catalog.AsyncReplication{
						heldReplication("mirror", "ra", catalog.ReplicationDone)},
					DeclaredReplications: []schemamodel.AsyncReplication{{Name: "mirror",
						Spec: replicationOf("mirror", "ra")}},
				},
			},
			wantErr: `table ra: it is a table async replication mirror created and failed over, which the schema ` +
				`keeps; .*`,
		},
		{
			name:    "a transfer into an undeclared table",
			caps:    capability.YDB262(),
			diff:    withTransfer(ingest, streams, table("orders")),
			wantErr: `transfer ingest: it writes table order_log, which the schema does not declare, .*`,
		},
		{
			name: "a transfer of a topic nobody declares",
			caps: capability.YDB262(),
			diff: withTransfer(ingest, schemaext.Objects{}, table("orders"), table("order_log")),
			wantErr: `transfer ingest: it reads topic orders/feed, which the schema declares neither as a topic ` +
				`nor as a changefeed, so the plan leaves no such topic, .*`,
		},
		{
			name: "a transfer's table changed",
			caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{TransfersModified: []difftypes.TransferDiff{{Name: "ingest",
				CreateOnlyChanged: []string{"target"}, Desired: retargeted, Current: ingest,
				State: catalog.ReplicationRunning}}},
			wantErr: `transfer ingest: its target differ from the database's, .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.NewWithCapabilities(test.caps).GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				test.diff,
			)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// TestGenerateMigrationAST_Replications_RebuildRefusesATransfersTable refuses
// to rebuild a table a transfer writes or reads a changefeed of: the rebuild
// swaps the table from under the transfer.
func TestGenerateMigrationAST_Replications_RebuildRefusesATransfersTable(t *testing.T) {
	typeChange := []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}}
	tests := []struct {
		name     string
		transfer ast.TransferSpec
	}{
		{name: "the table it writes", transfer: ast.TransferSpec{Source: "events", Target: "app/items",
			Lambda: lambda}},
		{name: "a changefeed it reads", transfer: ast.TransferSpec{Source: "app/items/feed", Target: "log",
			Lambda: lambda}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := modified(t, difftypes.TableDiff{TableName: "app.items",
				Desired:         appItems(field("label", "TEXT", true), field("n", "BIGINT", true)),
				ColumnsModified: typeChange})
			diff.Replications.CurrentTransfers = []catalog.Transfer{{Name: "ingest", State: catalog.ReplicationRunning,
				Spec: test.transfer}}

			nodes, err := ydb.NewWithCapabilities(capability.YDB262()).WithTableRebuild(true).GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				diff,
			)

			c.Assert(err, qt.ErrorMatches, `rebuilding table "app.items": transfer ingest writes the table or reads `+
				`one of its changefeeds, and a rebuild swaps the table from under it; drop the transfer, rebuild, `+
				`and create it again`)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
