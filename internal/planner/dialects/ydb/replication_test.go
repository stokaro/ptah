package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// primary is a connection to the database a replication reads.
var primary = ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod"}

// replicationOf is a replication of /prod's table source into target.
func replicationOf(source, target string) ydbreplication.ReplicationSpec {
	return ydbreplication.ReplicationSpec{Connection: primary,
		Items: []ydbreplication.Item{{Source: source, Target: target}}}
}

// heldReplication is a replication the database holds, in state.
func heldReplication(name, target, state string) schemaext.Object {
	return ydbreplication.ObservedReplicationObject("", name, replicationOf(name, target), state)
}

// objects collects feature objects.
func objects(values ...schemaext.Object) schemaext.Objects {
	return must.Must(schemaext.NewObjects(values...))
}

// replicationCreated is the creation of replication name.
func replicationCreated(name string, spec ydbreplication.ReplicationSpec) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: ydbreplication.ReplicationRef("", name),
		Value: ydbdiff.NewAsyncReplication(nil, &ydbreplication.DesiredReplication{Spec: spec})}
}

// replicationDropped is the drop of replication name, held in state.
func replicationDropped(name string, spec ydbreplication.ReplicationSpec, state string) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: ydbreplication.ReplicationRef("", name),
		Value: ydbdiff.NewAsyncReplication(&ydbreplication.ObservedReplication{Spec: spec, State: state}, nil)}
}

// replicationChanged is the change of replication name, held in state, from
// current to desired.
func replicationChanged(name string, current, desired ydbreplication.ReplicationSpec, state string) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: ydbreplication.ReplicationRef("", name),
		Value: ydbdiff.NewAsyncReplication(&ydbreplication.ObservedReplication{Spec: current, State: state},
			&ydbreplication.DesiredReplication{Spec: desired})}
}

// transferCreated is the creation of transfer name.
func transferCreated(name string, spec ydbreplication.TransferSpec) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: ydbreplication.TransferRef("", name),
		Value: ydbdiff.NewTransfer(nil, &ydbreplication.DesiredTransfer{Spec: spec})}
}

// transferDropped is the drop of transfer name.
func transferDropped(name string, spec ydbreplication.TransferSpec) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: ydbreplication.TransferRef("", name),
		Value: ydbdiff.NewTransfer(&ydbreplication.ObservedTransfer{Spec: spec, State: ydbreplication.StateRunning}, nil)}
}

// transferChanged is the change of transfer name from current to desired.
func transferChanged(name string, current, desired ydbreplication.TransferSpec) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: ydbreplication.TransferRef("", name),
		Value: ydbdiff.NewTransfer(&ydbreplication.ObservedTransfer{Spec: current, State: ydbreplication.StateRunning},
			&ydbreplication.DesiredTransfer{Spec: desired})}
}

// declaredTransfer is the declaration of transfer name.
func declaredTransfer(name string, spec ydbreplication.TransferSpec) schemaext.Object {
	return ydbreplication.DesiredTransferObject("", name, "", spec)
}

// heldTransfer is a running transfer the database holds.
func heldTransfer(name string, spec ydbreplication.TransferSpec) schemaext.Object {
	return ydbreplication.ObservedTransferObject("", name, spec, ydbreplication.StateRunning)
}

// replicationCoverage claims both namespaces in full.
func replicationCoverage(representation schemaext.Representation) schemaext.Coverage {
	complete := schemaext.Knowledge{State: schemaext.Complete}
	replications := must.Must(ydbreplication.ReplicationCoverage(representation, complete, nil))
	return must.Must(replications.Combine(must.Must(ydbreplication.TransferCoverage(representation, complete, nil))))
}

// table is a declared table with a key.
func table(name string) schemamodel.Table {
	return schemamodel.Table{StructName: "S", Name: name}
}

// lambda is the lambda every transfer below writes rows with.
const lambda = "($m) -> { return []; }"

// replicaOf is the read's record of the replica table at name.
func replicaOf(name string) coverage.Object {
	return coverage.Object{Kind: ydbschema.CoverageReplicaTable, Name: name, Reason: coverage.Unsupported,
		Provenance: coverage.Observed}
}

// TestGenerateMigrationAST_Replications_HappyPath pins where replications and
// transfers go in a YDB plan: every replication and transfer the plan removes
// goes first, a running replication with CASCADE and a failed-over one
// without; every one the plan creates or changes comes after the tables are
// created and dropped; and a view the plan creates waits for the replication
// that creates the replica table it reads, because YDB checks a view's query
// when it creates the view.
func TestGenerateMigrationAST_Replications_HappyPath(t *testing.T) {
	c := qt.New(t)
	feed := ydbschema.ChangefeedSpec{Name: "feed", Mode: "NEW_IMAGE", Format: "JSON"}
	moved := replicationOf("paused", "paused_copy")
	moved.Connection.ConnectionString = "grpc://standby:2136/?database=/prod"
	ingest := ydbreplication.TransferSpec{Source: "orders/feed", Target: "order_log", Lambda: lambda}
	relambda := ydbreplication.TransferSpec{Source: "orders/feed", Target: "order_log", Lambda: "($m) -> { return [1]; }"}
	feeds := declaredFeeds(t, schemacapture.TableDeclaration{Table: table("orders")}, feed).OwnedObjects
	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableChanges{{Name: "orders", Table: table("orders"),
			OwnedObjects: feeds,
			Fields:       []schemamodel.Field{keyField("id")}}},
		TablesRemoved: difftypes.TableRemovals{{Name: "legacy", Current: observedFeeds(t, "", "legacy")}},
		FeatureChanges: []schemaext.ChangeRecord{
			replicationCreated("mirror", replicationOf("accounts", "replica/accounts")),
			replicationDropped("failed_over", replicationOf("failed_over", "failed_over_copy"), ydbreplication.StateDone),
			replicationChanged("paused", replicationOf("paused", "paused_copy"), moved, ydbreplication.StatePaused),
			replicationDropped("running", replicationOf("running", "running_copy"), ydbreplication.StateRunning),
			transferCreated("ingest", ingest),
			transferDropped("old_ingest", ingest),
			transferChanged("reingest", ingest, relambda),
		},
		ViewsAdded: difftypes.ViewChanges{{Name: "v", Body: "SELECT id FROM `replica/accounts`"}},
		DeclaredTables: []schemamodel.Table{
			table("orders"), table("order_log"),
		},
		Features: difftypes.FeatureContext{
			DesiredObjects: must.Must(feeds.With(declaredTransfer("ingest", ingest))),
			CurrentObjects: objects(
				heldReplication("failed_over", "failed_over_copy", ydbreplication.StateDone),
				heldReplication("running", "running_copy", ydbreplication.StateRunning),
				heldReplication("paused", "paused_copy", ydbreplication.StatePaused),
			),
		},
	}
	diff.Features.DesiredObjects = must.Must(diff.Features.DesiredObjects.With(declaredTransfer("reingest", relambda)))

	got := render(c, capability.YDB262(), diff)

	c.Assert(got, qt.Equals, "DROP ASYNC REPLICATION `failed_over`;\n"+
		"DROP ASYNC REPLICATION `running` CASCADE;\n"+
		"DROP TRANSFER `old_ingest`;\n"+
		"CREATE TABLE `orders` (\n"+
		"    `id` Int64 NOT NULL,\n"+
		"    PRIMARY KEY (`id`)\n"+
		");\n"+
		"ALTER TABLE `orders` ADD CHANGEFEED `feed` WITH (MODE = 'NEW_IMAGE', FORMAT = 'JSON');\n"+
		"DROP TABLE `legacy`;\n"+
		"CREATE ASYNC REPLICATION `mirror` FOR `accounts` AS `replica/accounts` WITH ("+
		"CONNECTION_STRING = 'grpc://primary:2136/?database=/prod');\n"+
		"CREATE VIEW `v` WITH (security_invoker = TRUE) AS\n"+
		"SELECT id FROM `replica/accounts`\n"+
		";\n"+
		"ALTER ASYNC REPLICATION `paused` SET (CONNECTION_STRING = 'grpc://standby:2136/?database=/prod');\n"+
		"CREATE TRANSFER `ingest` FROM `orders/feed` TO `order_log` USING ($m) -> { return []; };\n"+
		"ALTER TRANSFER `reingest` SET USING ($m) -> { return [1]; };\n")
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
		Tables:         []schemamodel.Table{table("src")},
		Fields:         []schemamodel.Field{keyField("id")},
		FeatureObjects: objects(ydbreplication.DesiredReplicationObject("", "mirror", "", spec)),
	}
	read := &catalog.Database{
		Tables: []catalog.Table{{Name: "src", Type: "TABLE", Columns: []catalog.Column{{Name: "id",
			DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1}}}},
		Constraints: []catalog.Constraint{{Name: "src_pkey", TableName: "src", Type: "PRIMARY KEY",
			ColumnName: "id", ColumnNames: []string{"id"}}},
		FeatureObjects: objects(ydbreplication.ObservedReplicationObject("", "mirror", ydbreplication.ReplicationSpec{
			Connection: spec.Connection, Items: []ydbreplication.Item{{Source: "src", Target: "rep"}}},
			ydbreplication.StateRunning)),
		NotDescribed: coverage.Set{}.With(replicaOf("rep")),
	}
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	declared.FeatureCoverage = must.Must(feedCoverage(t, schemaext.Desired).Combine(replicationCoverage(schemaext.Desired)))
	read.FeatureCoverage = must.Must(feedCoverage(t, schemaext.Observed, schemaext.SubjectCoverage{
		Kind: ydbschema.ChangefeedKind, Subject: ydbschema.ChangefeedRef("", "src", "2b1f0c5e-0d1c-4b1a-9c3e-5d6f7a8b9c0d"),
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "replication stream"}}).
		Combine(replicationCoverage(schemaext.Observed)))
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
		{name: "a topic", source: "events", tables: []schemamodel.Table{table("order_log")},
			featureCoverage: must.Must(ydbtopic.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{
				{Kind: ydbtopic.Kind, Subject: ydbtopic.Ref("", "events"), Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: ydbtopic.QueueGroupReason}}}))},
		{name: "a changefeed in a directory", source: "app/orders/feed", tables: []schemamodel.Table{table("order_log"), {Schema: "app", Name: "orders"}},
			featureCoverage: feedCoverage(t, schemaext.Observed, schemaext.SubjectCoverage{Kind: ydbschema.ChangefeedKind, Subject: ydbschema.ChangefeedRef("app", "orders", "feed"), Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unsupported stream"}})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := ydbreplication.TransferSpec{Source: test.source, Target: "order_log", Lambda: lambda}
			diff := &difftypes.SchemaDiff{
				FeatureChanges:      []schemaext.ChangeRecord{transferCreated("ingest", spec)},
				DeclaredTables:      test.tables,
				CurrentNotDescribed: test.limits,
				Features: difftypes.FeatureContext{
					CurrentCoverage: test.featureCoverage,
					DesiredObjects:  objects(declaredTransfer("ingest", spec)),
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
	spec := ydbreplication.TransferSpec{Source: "app/events", Target: "order_log", Lambda: lambda}
	t.Run("created after its topic", func(t *testing.T) {
		c := qt.New(t)
		diff := &difftypes.SchemaDiff{
			FeatureChanges: []schemaext.ChangeRecord{{Subject: ydbtopic.Ref("app", "events"), Value: &ydbdiff.Topic{After: &ydbtopic.Desired{}}},
				transferCreated("ingest", spec)},
			DeclaredTables: []schemamodel.Table{table("order_log")},
			Features: difftypes.FeatureContext{
				DesiredObjects: objects(declaredTransfer("ingest", spec), ydbtopic.DesiredObject("app", "events", "", ydbtopic.Spec{})),
			},
		}

		got := render(c, capability.YDB262(), diff)

		c.Assert(got, qt.Equals, "CREATE TOPIC `app/events`;\n"+
			"CREATE TRANSFER `ingest` FROM `app/events` TO `order_log` USING ($m) -> { return []; };\n")
	})
	t.Run("its topic dropped", func(t *testing.T) {
		c := qt.New(t)
		diff := &difftypes.SchemaDiff{
			FeatureChanges: []schemaext.ChangeRecord{{Subject: ydbtopic.Ref("app", "events"), Value: &ydbdiff.Topic{Before: &ydbtopic.Observed{}}}},
			DeclaredTables: []schemamodel.Table{table("order_log")},
			Features: difftypes.FeatureContext{
				CurrentObjects: objects(heldTransfer("ingest", spec), ydbtopic.ObservedObject("app", "events", ydbtopic.Spec{})),
				DesiredObjects: objects(declaredTransfer("ingest", spec)),
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
	spec := ydbreplication.TransferSpec{Source: "events", Target: "order_log", Lambda: lambda}
	diff := &difftypes.SchemaDiff{
		FeatureChanges: []schemaext.ChangeRecord{{Subject: ydbtopic.Ref("", "events"), Value: &ydbdiff.Topic{Before: &ydbtopic.Observed{}}},
			transferDropped("ingest", spec)},
		Features: difftypes.FeatureContext{
			CurrentObjects: objects(heldTransfer("ingest", spec), ydbtopic.ObservedObject("", "events", ydbtopic.Spec{})),
		},
	}

	got := render(c, capability.YDB262(), diff)

	c.Assert(got, qt.Equals, "DROP TRANSFER `ingest`;\nDROP TOPIC `events`;\n")
}

// TestGenerateMigrationAST_Replications_SecretTakesTheTopicsPath drops a
// transfer and the topic it reads and creates a secret at the topic's path:
// the topic drop waits for the transfer's, and the secret, which runs as early
// as it can, waits for the topic's. Pinning the secret ahead of the first
// common statement made this plan a dependency cycle.
func TestGenerateMigrationAST_Replications_SecretTakesTheTopicsPath(t *testing.T) {
	c := qt.New(t)
	spec := ydbreplication.TransferSpec{Source: "events", Target: "order_log", Lambda: lambda}
	diff := &difftypes.SchemaDiff{
		FeatureChanges: []schemaext.ChangeRecord{
			{Subject: ydbtopic.Ref("", "events"), Value: &ydbdiff.Topic{Before: &ydbtopic.Observed{}}},
			secretCreated("", "events", "PTAH_SECRET_EVENTS"),
			transferDropped("ingest", spec),
		},
		Features: difftypes.FeatureContext{
			CurrentObjects: objects(heldTransfer("ingest", spec), ydbtopic.ObservedObject("", "events", ydbtopic.Spec{})),
		},
	}

	got := render(c, capability.YDB262(), diff)

	c.Assert(got, qt.Equals, "DROP TRANSFER `ingest`;\nDROP TOPIC `events`;\nCREATE SECRET `events` WITH (value = $PTAH_SECRET_EVENTS);\n")
}

// TestGenerateMigrationAST_Replications_FailurePath refuses, before any
// statement, a plan YDB cannot run or one that would break what a replication
// or a transfer owns or depends on.
func TestGenerateMigrationAST_Replications_FailurePath(t *testing.T) {
	ingest := ydbreplication.TransferSpec{Source: "orders/feed", Target: "order_log", Lambda: lambda}
	feed := ydbschema.ChangefeedSpec{Name: "feed", Mode: "UPDATES", Format: "JSON"}
	streams := declaredFeeds(t, schemacapture.TableDeclaration{Table: table("orders")}, feed).OwnedObjects
	withTransfer := func(spec ydbreplication.TransferSpec, declared schemaext.Objects, tables ...schemamodel.Table) *difftypes.SchemaDiff {
		return &difftypes.SchemaDiff{
			FeatureChanges: []schemaext.ChangeRecord{transferCreated("ingest", spec)},
			DeclaredTables: tables,
			Features: difftypes.FeatureContext{
				DesiredObjects: must.Must(declared.With(declaredTransfer("ingest", spec))),
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
			diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{
				replicationChanged("mirror", replicationOf("a", "ra"), global, ydbreplication.StatePaused)}},
			wantErr: `async replication mirror: its consistency_level differ from the database's, .*`,
		},
		{
			name: "a running replication's connection changed",
			caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{
				replicationChanged("mirror", replicationOf("a", "ra"), moved, ydbreplication.StateRunning)}},
			wantErr: `async replication mirror: its connection or credential differs, and YDB changes them only while ` +
				`the replication is paused .*`,
		},
		{
			name: "a running replication dropped while the schema declares its replica",
			caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{
				FeatureChanges: []schemaext.ChangeRecord{
					replicationDropped("mirror", replicationOf("a", "ra"), ydbreplication.StateRunning)},
				DeclaredTables: []schemamodel.Table{table("ra")},
				Features: difftypes.FeatureContext{CurrentObjects: objects(
					heldReplication("mirror", "ra", ydbreplication.StateRunning))},
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
				Features: difftypes.FeatureContext{CurrentObjects: objects(
					heldReplication("mirror", "ra", ydbreplication.StateRunning))},
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
				FeatureChanges:      []schemaext.ChangeRecord{replicationCreated("second", replicationOf("b", "replica"))},
				CurrentNotDescribed: coverage.Set{}.With(replicaOf("replica.accounts")),
			},
			wantErr: `async replication second: its target replica holds replica/accounts, a replica table of ` +
				`another replication, .*`,
		},
		{
			name: "a failed-over replica dropped while the schema keeps its replication",
			caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{
				TablesRemoved: difftypes.TableRemovals{{Name: "ra", Current: observedFeeds(t, "", "ra")}},
				Features: difftypes.FeatureContext{
					CurrentObjects: objects(heldReplication("mirror", "ra", ydbreplication.StateDone)),
					DesiredObjects: objects(ydbreplication.DesiredReplicationObject("", "mirror", "", replicationOf("mirror", "ra"))),
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
			diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{
				transferChanged("ingest", ingest, retargeted)}},
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
// swaps the table from under the transfer. YDB reports a source absolute, and
// an absolute source under the database names the same changefeed.
func TestGenerateMigrationAST_Replications_RebuildRefusesATransfersTable(t *testing.T) {
	typeChange := []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}}
	tests := []struct {
		name     string
		transfer ydbreplication.TransferSpec
	}{
		{name: "the table it writes", transfer: ydbreplication.TransferSpec{Source: "events", Target: "app/items",
			Lambda: lambda}},
		{name: "a changefeed it reads", transfer: ydbreplication.TransferSpec{Source: "app/items/feed", Target: "log",
			Lambda: lambda}},
		{name: "a changefeed it reads by its absolute path", transfer: ydbreplication.TransferSpec{Source: "/local/app/items/feed", Target: "log",
			Lambda: lambda}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := modified(t, difftypes.TableDiff{TableName: "app.items",
				Desired:         appItems(field("label", "TEXT", true), field("n", "BIGINT", true)),
				ColumnsModified: typeChange})
			diff.Features.CurrentObjects = objects(heldTransfer("ingest", test.transfer))
			diff.CurrentDatabasePath = "/local"

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
