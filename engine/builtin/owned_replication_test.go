package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// complete is the knowledge of a source that describes a namespace in full.
var complete = schemaext.Knowledge{State: schemaext.Complete}

// ownedReplicationSchema declares what [replicationSchema] declares, with the
// replication and the transfer as owned feature objects.
func ownedReplicationSchema(t *testing.T) *schemamodel.Database {
	common := replicationSchema(t)
	objects := must.Must(common.FeatureObjects.With(ydbreplication.DesiredReplicationObject("", "mirror", "", common.AsyncReplications[0].Spec)))
	objects = must.Must(objects.With(ydbreplication.DesiredTransferObject("", "ingest", "", common.Transfers[0].Spec)))
	return &schemamodel.Database{Tables: common.Tables, Fields: common.Fields, FeatureObjects: objects,
		FeatureCoverage: replicationCoverage(schemaext.Desired)}
}

// replicationCoverage claims both namespaces in full.
func replicationCoverage(representation schemaext.Representation) schemaext.Coverage {
	replications := must.Must(ydbreplication.ReplicationCoverage(representation, complete, nil))
	return must.Must(replications.Combine(must.Must(ydbreplication.TransferCoverage(representation, complete, nil))))
}

// TestRender_OwnedReplication_HappyPath writes an owned replication and an
// owned transfer as the common path writes the same declaration: after every
// table, and the transfer after the changefeed whose topic it reads.
func TestRender_OwnedReplication_HappyPath(t *testing.T) {
	c := qt.New(t)

	owned, err := builtin.GetOrderedCreateStatementsWithCapabilities(ownedReplicationSchema(t), platform.YDB, capability.YDB262())
	c.Assert(err, qt.IsNil)
	common, err := builtin.GetOrderedCreateStatementsWithCapabilities(replicationSchema(t), platform.YDB, capability.YDB262())
	c.Assert(err, qt.IsNil)

	c.Assert(owned, qt.DeepEquals, []string{
		"CREATE TABLE `orders` (\n    `id` Int32 NOT NULL,\n    PRIMARY KEY (`id`)\n);\n" +
			"ALTER TABLE `orders` ADD CHANGEFEED `feed` WITH (MODE = 'NEW_IMAGE', FORMAT = 'JSON');\n",
		"CREATE TABLE `order_log` (\n    `id` Int32 NOT NULL,\n    PRIMARY KEY (`id`)\n);\n",
		"CREATE ASYNC REPLICATION `mirror` FOR `accounts` AS `replica/accounts` WITH (" +
			"CONNECTION_STRING = 'grpc://primary:2136/?database=/prod', TOKEN_SECRET_NAME = 'token');\n",
		"CREATE TRANSFER `ingest` FROM `orders/feed` TO `order_log` USING ($m) -> { return []; };\n",
	})
	c.Assert(owned, qt.DeepEquals, common)
}

// TestRender_OwnedReplication_FailurePath refuses an owned replication or
// transfer on a YDB line without its key, a declaration a line cannot hold,
// a table declared at a replica's path, and either object on every other
// target, which registers no owner for it.
func TestRender_OwnedReplication_FailurePath(t *testing.T) {
	bySecretPath := ownedReplicationSchema(t)
	spec := replicationSchema(t).AsyncReplications[0].Spec
	spec.Connection.TokenSecretName, spec.Connection.TokenSecretPath = "", "secrets/token"
	bySecretPath.FeatureObjects = must.Must(bySecretPath.FeatureObjects.Replace(ydbreplication.DesiredReplicationObject("", "mirror", "", spec)))
	atAReplica := ownedReplicationSchema(t)
	atAReplica.Tables = append(atAReplica.Tables, schemamodel.Table{StructName: "Accounts", Name: "accounts", Schema: "replica", PrimaryKey: []string{"id"}})
	atAReplica.Fields = append(atAReplica.Fields, schemamodel.Field{StructName: "Accounts", Name: "id", Type: "int", Primary: true})
	type target struct {
		name    string
		dialect string
		caps    capability.Capabilities
		schema  *schemamodel.Database
		wantErr string
		wantIs  error
	}
	tests := []target{
		{name: "ydb without async replication", dialect: platform.YDB, caps: capability.YDB262().With(capability.AsyncReplication, false),
			schema:  ownedReplicationSchema(t),
			wantErr: `.*async replication mirror, which requires target capability async_replication, unavailable on this ydb target`,
			wantIs:  ptaherr.ErrUnsupportedFeature},
		{name: "a transfer on 25.1", dialect: platform.YDB, caps: capability.YDB251(), schema: ownedReplicationSchema(t),
			wantErr: `.*transfer ingest, which requires target capability transfers, unavailable on this ydb target`,
			wantIs:  ptaherr.ErrUnsupportedFeature},
		{name: "a secret path on 25.3", dialect: platform.YDB, caps: capability.YDB253(), schema: bySecretPath,
			wantErr: `.*async replication mirror names a secret by its path, which requires target capability ` +
				`replication_secret_paths, unavailable on this ydb target`,
			wantIs: ptaherr.ErrUnsupportedFeature},
		{name: "a table at a replica's path", dialect: platform.YDB, caps: capability.YDB262(), schema: atAReplica,
			wantErr: `.*conflicting plan effects: ptah\.run/ydb/scheme-path replica\.accounts has writers "ptah\.run/schema-creation" and "ptah\.run/ydb"`,
			wantIs:  ptaherr.ErrInvalidSchemaDiff},
	}
	for _, other := range secretlessTargets {
		tests = append(tests, target{name: other.dialect, dialect: other.dialect,
			caps:    other.caps.With(capability.AsyncReplication, true).With(capability.Transfers, true),
			schema:  ownedReplicationSchema(t),
			wantErr: `unsupported feature: feature objects are not registered for target "` + other.dialect + `"`,
			wantIs:  ptaherr.ErrUnsupportedFeature})
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(test.schema, test.dialect, test.caps)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// running and paused replication specs, and a transfer, for the operation
// tests.
var (
	mirrorSpec = ydbreplication.ReplicationSpec{
		Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod"},
		Items:      []ydbreplication.Item{{Source: "a", Target: "ra"}},
	}
	movedMirrorSpec = ydbreplication.ReplicationSpec{
		Connection: ydbreplication.Connection{ConnectionString: "grpc://standby:2136/?database=/prod"},
		Items:      []ydbreplication.Item{{Source: "a", Target: "ra"}},
	}
	ingestSpec   = ydbreplication.TransferSpec{Source: "tp", Target: "t", Lambda: "($m) -> { return []; }"}
	relambdaSpec = ydbreplication.TransferSpec{Source: "tp", Target: "t", Lambda: "($m) -> { return [<| id: 1 |>]; }"}
)

// TestRender_ReplicationOperation_HappyPath renders each statement on a
// replication and a transfer through YDB's owner: a drop takes the replica
// tables along unless the replication was failed over, and a change in place
// names only what differs.
func TestRender_ReplicationOperation_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		payload ast.ExtensionPayload
		want    string
	}{
		{name: "a replication creation", payload: &ydbast.AsyncReplication{Schema: "app", Name: "mirror",
			Change: ydbdiff.AsyncReplication{After: &ydbreplication.DesiredReplication{Spec: mirrorSpec}}},
			want: "CREATE ASYNC REPLICATION `app/mirror` FOR `a` AS `ra` WITH (CONNECTION_STRING = 'grpc://primary:2136/?database=/prod');\n"},
		{name: "a connection of a paused replication", payload: &ydbast.AsyncReplication{Name: "mirror",
			Change: ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: mirrorSpec, State: ydbreplication.StatePaused},
				After: &ydbreplication.DesiredReplication{Spec: movedMirrorSpec}}},
			want: "ALTER ASYNC REPLICATION `mirror` SET (CONNECTION_STRING = 'grpc://standby:2136/?database=/prod');\n"},
		{name: "a running replication's drop", payload: &ydbast.AsyncReplication{Name: "mirror",
			Change: ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: mirrorSpec, State: ydbreplication.StateRunning}}},
			want: "DROP ASYNC REPLICATION `mirror` CASCADE;\n"},
		{name: "a failed-over replication's drop", payload: &ydbast.AsyncReplication{Name: "mirror",
			Change: ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: mirrorSpec, State: ydbreplication.StateDone}}},
			want: "DROP ASYNC REPLICATION `mirror`;\n"},
		{name: "a transfer creation", payload: &ydbast.Transfer{Schema: "etl", Name: "ingest",
			Change: ydbdiff.Transfer{After: &ydbreplication.DesiredTransfer{Spec: ingestSpec}}},
			want: "CREATE TRANSFER `etl/ingest` FROM `tp` TO `t` USING ($m) -> { return []; };\n"},
		{name: "a transfer's lambda", payload: &ydbast.Transfer{Name: "ingest",
			Change: ydbdiff.Transfer{Before: &ydbreplication.ObservedTransfer{Spec: ingestSpec, State: ydbreplication.StateRunning},
				After: &ydbreplication.DesiredTransfer{Spec: relambdaSpec}}},
			want: "ALTER TRANSFER `ingest` SET USING ($m) -> { return [<| id: 1 |>]; };\n"},
		{name: "a transfer's drop", payload: &ydbast.Transfer{Schema: "etl", Name: "ingest",
			Change: ydbdiff.Transfer{Before: &ydbreplication.ObservedTransfer{Spec: ingestSpec}}},
			want: "DROP TRANSFER `etl/ingest`;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql, err := builtin.RenderSQLWithCapabilities(platform.YDB, capability.YDB262(), &ast.ExtensionStatement{Payload: test.payload})
			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}

// TestRender_ReplicationOperation_FailurePath refuses a change YDB cannot make
// in the state the replication reported, and one it makes in no state, before
// anything is written.
func TestRender_ReplicationOperation_FailurePath(t *testing.T) {
	reitemed := mirrorSpec.Clone()
	reitemed.Items = []ydbreplication.Item{{Source: "b", Target: "rb"}}
	retargeted := ingestSpec
	retargeted.Target = "other"
	tests := []struct {
		name    string
		payload ast.ExtensionPayload
		want    string
	}{
		{name: "a running replication's connection", payload: &ydbast.AsyncReplication{Name: "mirror",
			Change: ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: mirrorSpec, State: ydbreplication.StateRunning},
				After: &ydbreplication.DesiredReplication{Spec: movedMirrorSpec}}},
			want: ".*async replication mirror: its connection or credential differs, and YDB changes them only while " +
				"the replication is paused .*"},
		{name: "a replication's items", payload: &ydbast.AsyncReplication{Name: "mirror",
			Change: ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: mirrorSpec, State: ydbreplication.StatePaused},
				After: &ydbreplication.DesiredReplication{Spec: reitemed}}},
			want: ".*async replication mirror: its items differ from the database's, and YDB changes none of them in place .*"},
		{name: "a transfer's target", payload: &ydbast.Transfer{Name: "ingest",
			Change: ydbdiff.Transfer{Before: &ydbreplication.ObservedTransfer{Spec: ingestSpec, State: ydbreplication.StatePaused},
				After: &ydbreplication.DesiredTransfer{Spec: retargeted}}},
			want: ".*transfer ingest: its target differ from the database's, and YDB changes none of them in place .*"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql, err := builtin.RenderSQLWithCapabilities(platform.YDB, capability.YDB262(), &ast.ExtensionStatement{Payload: test.payload})
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestRender_ReplicationOperation_NonOwningRenderersRefuse hands each
// statement to every renderer but YDB's: each refuses the payload through the
// common extension boundary without knowing what it is.
func TestRender_ReplicationOperation_NonOwningRenderersRefuse(t *testing.T) {
	for _, test := range secretlessTargets {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			for _, operation := range []struct {
				payload ast.ExtensionPayload
				kind    string
			}{
				{payload: &ydbast.AsyncReplication{Name: "mirror",
					Change: ydbdiff.AsyncReplication{After: &ydbreplication.DesiredReplication{Spec: mirrorSpec}}},
					kind: `ptah\.run/ydb/async-replication-operation`},
				{payload: &ydbast.Transfer{Name: "ingest",
					Change: ydbdiff.Transfer{Before: &ydbreplication.ObservedTransfer{Spec: ingestSpec}}},
					kind: `ptah\.run/ydb/transfer-operation`},
			} {
				caps := test.caps.With(capability.AsyncReplication, true).With(capability.Transfers, true)
				sql, err := builtin.RenderSQLWithCapabilities(test.dialect, caps, &ast.ExtensionStatement{Payload: operation.payload})
				c.Assert(err, qt.ErrorMatches, `target "`+test.dialect+`" does not support extension "`+operation.kind+`" in role "statement"`)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(sql, qt.Equals, "")
			}
		})
	}
}

// ownedPlan compares desired with current on YDB 26.2 and plans the result.
func ownedPlan(c *qt.C, desired *schemamodel.Database, current *catalog.Database) ([]string, error) {
	c.Helper()
	runtime := must.Must(builtin.New())
	info := catalog.ServerInfo{Dialect: platform.YDB, Capabilities: capability.YDB262()}
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current, info, nil, runtime)
	if err != nil {
		return nil, err
	}
	return planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), runtime, diff, platform.YDB,
		planner.Options{Capabilities: capability.YDB262()})
}

// TestPlan_OwnedTransfer_DroppedBeforeItsTopic drops a transfer before the
// topic it reads. Both drops run before the common statements, and the topic
// owner's would run first by name; the transfer's read of its topic orders it
// first, so the topic is never gone under a transfer that reads it.
func TestPlan_OwnedTransfer_DroppedBeforeItsTopic(t *testing.T) {
	c := qt.New(t)
	topics := must.Must(ydbtopic.Coverage(schemaext.Desired, complete, nil))
	desired := &schemamodel.Database{FeatureCoverage: must.Must(replicationCoverage(schemaext.Desired).Combine(topics))}
	observedTopics := must.Must(ydbtopic.Coverage(schemaext.Observed, complete, nil))
	current := &catalog.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(
			ydbtopic.ObservedObject("", "events", ydbtopic.Spec{}),
			ydbreplication.ObservedTransferObject("", "ingest", ydbreplication.TransferSpec{Source: "events", Target: "log",
				Lambda: "($m) -> { return []; }", Consumer: "ingest"}, ydbreplication.StateRunning),
		)),
		FeatureCoverage: must.Must(replicationCoverage(schemaext.Observed).Combine(observedTopics)),
	}

	statements, err := ownedPlan(c, desired, current)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{"DROP TRANSFER `ingest`", "DROP TOPIC `events`"})
}

// TestPlan_OwnedReplication_RefusesADroppedSecretItReads refuses a plan that
// creates a replication naming a secret by its path while the secret's owner
// drops it: YDB records no dependency on a secret, so the replication would
// name one that is gone.
func TestPlan_OwnedReplication_RefusesADroppedSecretItReads(t *testing.T) {
	c := qt.New(t)
	spec := mirrorSpec.Clone()
	spec.Connection.TokenSecretPath = "secrets/token"
	secrets := must.Must(ydbsecret.Coverage(schemaext.Desired, complete, nil))
	desired := &schemamodel.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(ydbreplication.DesiredReplicationObject("", "mirror", "", spec))),
		FeatureCoverage: must.Must(replicationCoverage(schemaext.Desired).Combine(secrets)),
	}
	observedSecrets := must.Must(ydbsecret.Coverage(schemaext.Observed, complete, nil))
	current := &catalog.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(ydbsecret.ObservedObject("secrets", "token"))),
		FeatureCoverage: must.Must(replicationCoverage(schemaext.Observed).Combine(observedSecrets)),
	}

	statements, err := ownedPlan(c, desired, current)

	c.Assert(err, qt.ErrorMatches, `.*secret secrets/token is dropped while a statement of this plan reads it by its path`)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(statements, qt.IsNil)
}

// TestPlan_OwnedReplication_CreatedAfterTheTableAtItsTarget drops the table at
// a replication's target before the replication creates its replica there,
// and creates a secret the replication reads before it.
func TestPlan_OwnedReplication_CreatedAfterTheTableAtItsTarget(t *testing.T) {
	c := qt.New(t)
	spec := mirrorSpec.Clone()
	spec.Connection.TokenSecretPath = "secrets/token"
	secrets := must.Must(ydbsecret.Coverage(schemaext.Desired, complete, nil))
	desired := &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(
			ydbreplication.DesiredReplicationObject("", "mirror", "", spec),
			ydbsecret.DesiredObject("secrets", "token", "", "PTAH_SECRET_TOKEN"),
		)),
		FeatureCoverage: must.Must(replicationCoverage(schemaext.Desired).Combine(secrets)),
	}
	observedSecrets := must.Must(ydbsecret.Coverage(schemaext.Observed, complete, nil))
	changefeeds := must.Must(ydbschema.ChangefeedCoverage(schemaext.Observed, nil))
	current := &catalog.Database{
		Tables:          []catalog.Table{{Name: "ra"}},
		FeatureCoverage: must.Must(must.Must(replicationCoverage(schemaext.Observed).Combine(observedSecrets)).Combine(changefeeds)),
	}

	statements, err := ownedPlan(c, desired, current)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"CREATE SECRET `secrets/token` WITH (value = $PTAH_SECRET_TOKEN)",
		"DROP TABLE `ra`",
		"CREATE ASYNC REPLICATION `mirror` FOR `a` AS `ra` WITH (CONNECTION_STRING = 'grpc://primary:2136/?database=/prod', " +
			"TOKEN_SECRET_PATH = 'secrets/token')",
	})
}

// TestValidateSchema_OwnedReplication holds each owned declaration to the
// rules its creation is rendered with, so validation refuses what the render
// would refuse, and the same declaration validates on a line that has every
// key.
func TestValidateSchema_OwnedReplication(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		wantErr string
	}{
		{name: "a transfer on 25.1", caps: capability.YDB251(),
			wantErr: `.*transfer ingest, which requires target capability transfers, unavailable on this ydb target.*`},
		{name: "a replication without the key", caps: capability.YDB262().With(capability.AsyncReplication, false),
			wantErr: `.*async replication mirror, which requires target capability async_replication, unavailable on this ydb target.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			result, err := runtime.ValidateSchema(t.Context(), schemavalidation.Request{Target: "ydb", Capabilities: test.caps,
				Schema: ownedReplicationSchema(t), NoSkipped: true})
			c.Assert(err, qt.IsNil)
			c.Assert(result.Err("ydb"), qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestValidateSchema_OwnedReplicationOnAFullLine is the control: a line with
// every key validates the declaration.
func TestValidateSchema_OwnedReplicationOnAFullLine(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	result, err := runtime.ValidateSchema(t.Context(), schemavalidation.Request{Target: "ydb", Capabilities: capability.YDB262(),
		Schema: ownedReplicationSchema(t), NoSkipped: true})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Err("ydb"), qt.IsNil)
}
