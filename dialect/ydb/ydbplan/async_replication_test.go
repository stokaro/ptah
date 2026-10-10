package ydbplan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine"
)

func replicationPlanningRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	codecs := append(ydbreplication.Codecs(), ydbdiff.AsyncReplicationCodec(), ydbdiff.TransferCodec(),
		ydbast.AsyncReplicationCodec(), ydbast.TransferCodec())
	runtime, err := engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}}, Codecs: codecs,
		Declarations: []engine.DeclarationPlanning{
			{Target: "ydb", Kinds: []schemaext.Kind{ydbreplication.ReplicationKind}, OperationKinds: []schemaext.Kind{ydbast.AsyncReplicationKind},
				Service: ydbplan.AsyncReplicationService{}},
			{Target: "ydb", Kinds: []schemaext.Kind{ydbreplication.TransferKind}, OperationKinds: []schemaext.Kind{ydbast.TransferKind},
				Service: ydbplan.TransferService{}},
		},
		Planning: []engine.Planning{
			{Target: "ydb", Kinds: []schemaext.Kind{ydbdiff.AsyncReplicationKind}, OperationKinds: []schemaext.Kind{ydbast.AsyncReplicationKind},
				Service: ydbplan.AsyncReplicationService{}},
			{Target: "ydb", Kinds: []schemaext.Kind{ydbdiff.TransferKind}, OperationKinds: []schemaext.Kind{ydbast.TransferKind},
				Service: ydbplan.TransferService{}},
		}})
	c.Assert(err, qt.IsNil)
	return runtime
}

var (
	plannedMirror = ydbreplication.ReplicationSpec{
		Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod",
			User: "replicator", PasswordSecretPath: "secrets/replicator"},
		Items: []ydbreplication.Item{{Source: "a", Target: "replica/a"}, {Source: "b", Target: "replica/b"}},
	}
	plannedIngest = ydbreplication.TransferSpec{Source: "app/orders/updates", Target: "app/log", Lambda: "($m) -> { return []; }"}
)

// replicationRequest plans changes in the database /local on YDB 26.2.
func replicationRequest(changes ...schemaext.ChangeRecord) featureplan.Request {
	return featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
		DatabasePath: "/local", Changes: changes}
}

// TestReplicationPlan_EffectsAndPlacement gives each statement the effects the
// host orders it by, and runs a drop early and anything else after the common
// statements. A creation creates a replica at each target and reads the
// secret its connection names by path; a drop with CASCADE drops the
// replicas, and one of a failed-over replication touches nothing else; a
// transfer reads its table and its topic, which a source path may name as a
// standalone topic or as a changefeed, and a drop reads what it used.
func TestReplicationPlan_EffectsAndPlacement(t *testing.T) {
	mirror := ydbreplication.ReplicationRef("", "mirror")
	ingest := ydbreplication.TransferRef("etl", "ingest")
	secret := ydbsecret.Ref("secrets", "replicator")
	transferReads := []plangraph.Effect{
		{Subject: ydbscheme.Path("app", "log"), Action: plangraph.Read},
		{Subject: ydbtopic.Ref("app/orders", "updates"), Action: plangraph.Read},
		{Subject: ydbschema.ChangefeedRef("app", "orders", "updates"), Action: plangraph.Read},
	}
	tests := []struct {
		name      string
		change    schemaext.ChangeRecord
		effects   []plangraph.Effect
		placement plangraph.Placement
		strategy  string
	}{
		{name: "a created replication", change: schemaext.ChangeRecord{Subject: mirror,
			Value: &ydbdiff.AsyncReplication{After: &ydbreplication.DesiredReplication{Spec: plannedMirror}}},
			effects: []plangraph.Effect{{Subject: mirror, Action: plangraph.Create}, {Subject: ydbscheme.Path("", "mirror"), Action: plangraph.Create},
				{Subject: secret, Action: plangraph.Read},
				{Subject: ydbscheme.Path("replica", "a"), Action: plangraph.Create}, {Subject: ydbscheme.Path("replica", "b"), Action: plangraph.Create}},
			strategy: "create the replication, which creates its replica tables and copies its source"},
		{name: "a running replication dropped", change: schemaext.ChangeRecord{Subject: mirror,
			Value: &ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: plannedMirror, State: ydbreplication.StateRunning}}},
			effects: []plangraph.Effect{{Subject: mirror, Action: plangraph.Drop}, {Subject: ydbscheme.Path("", "mirror"), Action: plangraph.Drop},
				{Subject: ydbscheme.Path("replica", "a"), Action: plangraph.Drop}, {Subject: ydbscheme.Path("replica", "b"), Action: plangraph.Drop}},
			placement: plangraph.PlacementEarly, strategy: "drop the replication with its replica tables"},
		{name: "a failed-over replication dropped", change: schemaext.ChangeRecord{Subject: mirror,
			Value: &ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: plannedMirror, State: ydbreplication.StateDone}}},
			effects:   []plangraph.Effect{{Subject: mirror, Action: plangraph.Drop}, {Subject: ydbscheme.Path("", "mirror"), Action: plangraph.Drop}},
			placement: plangraph.PlacementEarly, strategy: "drop the failed-over replication and keep its tables"},
		{name: "a paused replication moved", change: schemaext.ChangeRecord{Subject: mirror,
			Value: &ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: ydbreplication.ReplicationSpec{
				Connection: ydbreplication.Connection{ConnectionString: "grpc://standby:2136/?database=/prod"}, Items: plannedMirror.Items},
				State: ydbreplication.StatePaused}, After: &ydbreplication.DesiredReplication{Spec: plannedMirror}}},
			effects: []plangraph.Effect{{Subject: mirror, Action: plangraph.Alter}, {Subject: ydbscheme.Path("", "mirror"), Action: plangraph.Alter},
				{Subject: secret, Action: plangraph.Read}},
			strategy: "point the paused replication at the declared connection and credential"},
		{name: "a created transfer", change: schemaext.ChangeRecord{Subject: ingest,
			Value: &ydbdiff.Transfer{After: &ydbreplication.DesiredTransfer{Spec: plannedIngest}}},
			effects: append([]plangraph.Effect{{Subject: ingest, Action: plangraph.Create}, {Subject: ydbscheme.Path("etl", "ingest"), Action: plangraph.Create}},
				transferReads...),
			strategy: "create the transfer, which reads its topic into its table"},
		{name: "a dropped transfer", change: schemaext.ChangeRecord{Subject: ingest,
			Value: &ydbdiff.Transfer{Before: &ydbreplication.ObservedTransfer{Spec: plannedIngest}}},
			effects: append([]plangraph.Effect{{Subject: ingest, Action: plangraph.Drop}, {Subject: ydbscheme.Path("etl", "ingest"), Action: plangraph.Drop}},
				transferReads...),
			placement: plangraph.PlacementEarly, strategy: "drop the transfer and the consumer YDB created for it"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := replicationRequest(test.change)

			result, err := replicationPlanningRuntime(c).PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Err(request), qt.IsNil)
			c.Assert(result.Contributions, qt.HasLen, 1)
			c.Assert(result.Contributions[0].Steps, qt.HasLen, 1)
			step := result.Contributions[0].Steps[0]
			c.Assert(step.Effects, qt.DeepEquals, test.effects)
			c.Assert(step.Placement, qt.Equals, test.placement)
			c.Assert(step.Transaction, qt.Equals, plangraph.TransactionForbidden)
			c.Assert(result.Changes[0].Strategy, qt.Equals, test.strategy)
		})
	}
}

// TestReplicationPlan_ARemoteTransferReadsNoTopic reads no topic of this
// database for a transfer of another database's topic, and reads the secret
// its connection names by path.
func TestReplicationPlan_ARemoteTransferReadsNoTopic(t *testing.T) {
	c := qt.New(t)
	remote := plannedIngest
	remote.Connection = ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod", TokenSecretPath: "secrets/token"}
	ingest := ydbreplication.TransferRef("", "ingest")
	request := replicationRequest(schemaext.ChangeRecord{Subject: ingest,
		Value: &ydbdiff.Transfer{After: &ydbreplication.DesiredTransfer{Spec: remote}}})

	result, err := replicationPlanningRuntime(c).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Contributions[0].Steps[0].Effects, qt.DeepEquals, []plangraph.Effect{
		{Subject: ingest, Action: plangraph.Create}, {Subject: ydbscheme.Path("", "ingest"), Action: plangraph.Create},
		{Subject: ydbsecret.Ref("secrets", "token"), Action: plangraph.Read},
		{Subject: ydbscheme.Path("app", "log"), Action: plangraph.Read},
	})
}

// TestReplicationPlan_RefusesAChangeNamedTwice refuses a batch that names one
// object twice, which no comparison produces.
func TestReplicationPlan_RefusesAChangeNamedTwice(t *testing.T) {
	c := qt.New(t)
	mirror := ydbreplication.ReplicationRef("", "mirror")
	change := &ydbdiff.AsyncReplication{After: &ydbreplication.DesiredReplication{Spec: plannedMirror}}
	request := replicationRequest(schemaext.ChangeRecord{Subject: mirror, Value: change}, schemaext.ChangeRecord{Subject: mirror, Value: change})

	result, err := (ydbplan.AsyncReplicationService{}).PlanFeatures(t.Context(), request)

	c.Assert(err, qt.ErrorMatches, `.*unexpected or duplicate async replication change`)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result.Contributions, qt.HasLen, 0)
}

// TestReplicationPlan_Refusals refuses, before any operation is returned, a
// statement the target cannot take and a change YDB cannot make in the state
// the replication reported.
func TestReplicationPlan_Refusals(t *testing.T) {
	mirror := ydbreplication.ReplicationRef("", "mirror")
	moved := plannedMirror.Clone()
	moved.Connection.ConnectionString = "grpc://standby:2136/?database=/prod"
	tests := []struct {
		name    string
		caps    capability.Capabilities
		change  schemaext.ChangeValue
		wantErr string
	}{
		{name: "no key for a drop", caps: capability.YDB262().With(capability.AsyncReplication, false),
			change:  &ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: plannedMirror}},
			wantErr: `.*async replication mirror, which requires target capability async_replication, unavailable on this ydb target.*`},
		{name: "a secret path on 25.3", caps: capability.YDB253(),
			change:  &ydbdiff.AsyncReplication{After: &ydbreplication.DesiredReplication{Spec: plannedMirror}},
			wantErr: `.*async replication mirror names a secret by its path, which requires target capability replication_secret_paths.*`},
		{name: "a running replication moved", caps: capability.YDB262(),
			change: &ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: plannedMirror, State: ydbreplication.StateRunning},
				After: &ydbreplication.DesiredReplication{Spec: moved}},
			wantErr: `.*async replication mirror: its connection or credential differs, and YDB changes them only while the replication is paused.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := replicationRequest(schemaext.ChangeRecord{Subject: mirror, Value: test.change})
			request.Capabilities = test.caps

			result, err := replicationPlanningRuntime(c).PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Problem.Code, qt.Equals, schemavalidation.UnsupportedFeature)
			c.Assert(result.Err(request), qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestReplicationPlan_Declarations creates each declared object through the
// migration planner's rules.
func TestReplicationPlan_Declarations(t *testing.T) {
	c := qt.New(t)
	request := featureplan.DeclarationRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
		Objects: []schemaext.Object{
			ydbreplication.DesiredReplicationObject("", "mirror", "", plannedMirror),
			ydbreplication.DesiredTransferObject("etl", "ingest", "", plannedIngest),
		}}

	result, err := replicationPlanningRuntime(c).PlanDeclarations(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Err(request), qt.IsNil)
	var strategies []string
	for _, declaration := range result.Declarations {
		strategies = append(strategies, declaration.Strategy)
	}
	c.Assert(strategies, qt.DeepEquals, []string{"create the declared replication with its items", "create the declared transfer"})
}
