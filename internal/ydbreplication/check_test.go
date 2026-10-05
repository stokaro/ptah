package ydbreplication_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/ydbreplication"
)

// mirror is a replication of /prod's accounts table into a replica.
func mirror() ast.AsyncReplicationSpec {
	return ast.AsyncReplicationSpec{
		Connection: ast.ReplicationConnectionSpec{ConnectionString: "grpc://primary:2136/?database=/prod",
			TokenSecretName: "token"},
		Items: []ast.AsyncReplicationItem{{Source: "accounts", Target: "replica/accounts"}},
	}
}

// ingest is a transfer of a topic into a table.
func ingest() ast.TransferSpec {
	return ast.TransferSpec{Source: "orders/feed", Target: "order_log", Lambda: "($m) -> { return []; }"}
}

// TestCheckReplication_HappyPath accepts a replication each target that has
// the family can create, a secret named by its path where the line takes one.
func TestCheckReplication_HappyPath(t *testing.T) {
	byPath := mirror()
	byPath.Connection.TokenSecretName, byPath.Connection.TokenSecretPath = "", "secrets/token"
	tests := []struct {
		name string
		spec ast.AsyncReplicationSpec
		caps capability.Capabilities
	}{
		{name: "a token secret by name on 25.1", spec: mirror(), caps: capability.YDB251()},
		{name: "a token secret by path on 26.2", spec: byPath, caps: capability.YDB262()},
		{name: "a token secret by path on 25.4", spec: byPath, caps: capability.YDB254()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbreplication.CheckReplication("mirror", test.spec, test.caps), qt.IsNil)
		})
	}
}

// TestCheckReplication_FailurePath refuses a replication a target cannot
// create, naming the key it lacks or the reason YDB refuses it on every line.
func TestCheckReplication_FailurePath(t *testing.T) {
	byPath := mirror()
	byPath.Connection.TokenSecretName, byPath.Connection.TokenSecretPath = "", "secrets/token"
	noItems := mirror()
	noItems.Items = nil
	twoAtOnePlace := mirror()
	twoAtOnePlace.Items = append(twoAtOnePlace.Items, ast.AsyncReplicationItem{Source: "ledger",
		Target: "replica/accounts"})
	plaintext := mirror()
	plaintext.Connection.ConnectionString = "grpc://primary:2136/?database=/prod&password=x"
	tests := []struct {
		name string
		spec ast.AsyncReplicationSpec
		caps capability.Capabilities
		want ydbreplication.Refusal
	}{
		{name: "PostgreSQL", spec: mirror(), caps: capability.Postgres17(),
			want: ydbreplication.Refusal{Subject: "async replication mirror", Key: capability.AsyncReplication}},
		{name: "a secret path on 25.3", spec: byPath, caps: capability.YDB253(),
			want: ydbreplication.Refusal{Subject: "async replication mirror names a secret by its path",
				Key: capability.ReplicationSecretPaths}},
		{name: "no items", spec: noItems, caps: capability.YDB262(),
			want: ydbreplication.Refusal{Subject: "async replication mirror", Reason: "it replicates no table; " +
				"declare an item naming a source and a target, since YDB takes no replication without one " +
				"(`expecting {',', WITH}`)"}},
		{name: "two replicas at one place", spec: twoAtOnePlace, caps: capability.YDB262(),
			want: ydbreplication.Refusal{Subject: "async replication mirror",
				Reason: `two of its items create a replica at "replica/accounts", where one table can stand`}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := ydbreplication.CheckReplication("mirror", test.spec, test.caps)
			c.Assert(got, qt.IsNotNil)
			c.Assert(*got, qt.DeepEquals, test.want)
		})
	}
	t.Run("a connection string carrying a password", func(t *testing.T) {
		c := qt.New(t)
		got := ydbreplication.CheckReplication("mirror", plaintext, capability.YDB262())
		c.Assert(got, qt.IsNotNil)
		c.Assert(got.Key, qt.Equals, capability.Capability(""))
		c.Assert(got.Reason, qt.Matches, `invalid connection_string .*`)
	})
}

// TestCheckTransfer_HappyPath accepts a transfer on each line with the key.
func TestCheckTransfer_HappyPath(t *testing.T) {
	for _, caps := range []capability.Capabilities{capability.YDB262(), capability.YDB252()} {
		c := qt.New(t)
		c.Assert(ydbreplication.CheckTransfer("ingest", ingest(), caps), qt.IsNil)
	}
}

// TestCheckTransfer_FailurePath refuses a transfer on 25.1, where YDB
// answers `Topic transfer creation is disabled` unless a feature flag is set,
// and one naming a secret by path where the line takes none.
func TestCheckTransfer_FailurePath(t *testing.T) {
	remote := ingest()
	remote.Connection = ast.ReplicationConnectionSpec{ConnectionString: "grpc://primary:2136/?database=/prod",
		TokenSecretPath: "secrets/token"}
	tests := []struct {
		name string
		spec ast.TransferSpec
		caps capability.Capabilities
		want ydbreplication.Refusal
	}{
		{name: "25.1", spec: ingest(), caps: capability.YDB251(),
			want: ydbreplication.Refusal{Subject: "transfer ingest", Key: capability.Transfers}},
		{name: "a secret path on 25.2", spec: remote, caps: capability.YDB252(),
			want: ydbreplication.Refusal{Subject: "transfer ingest names a secret by its path",
				Key: capability.ReplicationSecretPaths}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := ydbreplication.CheckTransfer("ingest", test.spec, test.caps)
			c.Assert(got, qt.IsNotNil)
			c.Assert(*got, qt.DeepEquals, test.want)
		})
	}
}

// TestReplicationChangeRefusal_HappyPath lets through what YDB changes in
// place: nothing, or a connection or a credential of a paused replication.
func TestReplicationChangeRefusal_HappyPath(t *testing.T) {
	moved := mirror()
	moved.Connection.ConnectionString = "grpcs://standby:2135/?database=/prod"
	byUser := mirror()
	byUser.Connection.TokenSecretName, byUser.Connection.User, byUser.Connection.PasswordSecretName =
		"", "replicator", "password"
	tests := []struct {
		name    string
		desired ast.AsyncReplicationSpec
		state   string
	}{
		{name: "nothing differs while running", desired: mirror(), state: catalog.ReplicationRunning},
		{name: "nothing differs after failover", desired: mirror(), state: catalog.ReplicationDone},
		{name: "another host while paused", desired: moved, state: catalog.ReplicationPaused},
		{name: "another credential while paused", desired: byUser, state: catalog.ReplicationPaused},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbreplication.ReplicationChangeRefusal("mirror", test.desired, mirror(), test.state), qt.IsNil)
		})
	}
}

// TestReplicationChangeRefusal_FailurePath refuses what YDB changes in no
// replication, and a connection change YDB takes only while paused.
func TestReplicationChangeRefusal_FailurePath(t *testing.T) {
	moved := mirror()
	moved.Connection.ConnectionString = "grpcs://standby:2135/?database=/prod"
	global := mirror()
	global.ConsistencyLevel = "global"
	retargeted := mirror()
	retargeted.Items[0].Target = "copy/accounts"
	tokenless := mirror()
	tokenless.Connection.TokenSecretName = ""
	tests := []struct {
		name    string
		desired ast.AsyncReplicationSpec
		state   string
		want    string
	}{
		{name: "the level", desired: global, state: catalog.ReplicationPaused,
			want: "its consistency_level differ from the database's, and YDB changes none of them in place " +
				"(`CONSISTENCY_LEVEL is not supported in ALTER`, and ALTER takes no FOR clause); drop it with " +
				"DROP ASYNC REPLICATION ... CASCADE, which drops its replica tables, and plan again"},
		{name: "the items", desired: retargeted, state: catalog.ReplicationPaused,
			want: "its items differ from the database's, and YDB changes none of them in place " +
				"(`CONSISTENCY_LEVEL is not supported in ALTER`, and ALTER takes no FOR clause); drop it with " +
				"DROP ASYNC REPLICATION ... CASCADE, which drops its replica tables, and plan again"},
		{name: "a connection after failover", desired: moved, state: catalog.ReplicationDone,
			want: "it was failed over (STATE = 'DONE') and replicates nothing more, so its connection no longer " +
				"applies; remove it from the schema, which drops it and keeps its tables"},
		{name: "a credential taken away", desired: tokenless, state: catalog.ReplicationPaused,
			want: "it is declared with no credential and holds one, and YDB has no statement that takes a " +
				"credential away"},
		{name: "a connection while running", desired: moved, state: catalog.ReplicationRunning,
			want: "its connection or credential differs, and YDB changes them only while the replication is paused " +
				"(`Modifications are not allowed in StandBy state`), which it is not (it is running); pause it with " +
				"ALTER ASYNC REPLICATION `mirror` SET (STATE = 'PAUSED'), apply, and resume it with " +
				"SET (STATE = 'StandBy')"},
		{name: "a connection in error", desired: moved, state: catalog.ReplicationError,
			want: "its connection or credential differs, and YDB changes them only while the replication is paused " +
				"(`Modifications are not allowed in StandBy state`), which it is not (it is error); pause it with " +
				"ALTER ASYNC REPLICATION `mirror` SET (STATE = 'PAUSED'), apply, and resume it with " +
				"SET (STATE = 'StandBy')"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := ydbreplication.ReplicationChangeRefusal("mirror", test.desired, mirror(), test.state)
			c.Assert(got, qt.IsNotNil)
			c.Assert(got.Subject, qt.Equals, "async replication mirror")
			c.Assert(got.Reason, qt.Equals, test.want)
		})
	}
}

// TestTransferChangeRefusal_HappyPath lets through what YDB changes in any
// state -- the lambda and the batch settings -- and a connection change of a
// paused transfer.
func TestTransferChangeRefusal_HappyPath(t *testing.T) {
	relambda := ingest()
	relambda.Lambda = "($m) -> { return [<| id: 1 |>]; }"
	rebatched := ingest()
	rebatched.BatchSizeBytes = 1024
	remote := ingest()
	remote.Connection.ConnectionString = "grpc://primary:2136/?database=/prod"
	moved := remote
	moved.Connection.ConnectionString = "grpc://standby:2136/?database=/prod"
	tests := []struct {
		name    string
		desired ast.TransferSpec
		current ast.TransferSpec
		state   string
	}{
		{name: "the lambda while running", desired: relambda, current: ingest(), state: catalog.ReplicationRunning},
		{name: "the batch while running", desired: rebatched, current: ingest(), state: catalog.ReplicationRunning},
		{name: "another host while paused", desired: moved, current: remote, state: catalog.ReplicationPaused},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbreplication.TransferChangeRefusal("ingest", test.desired, test.current, test.state), qt.IsNil)
		})
	}
}

// TestTransferChangeRefusal_FailurePath refuses what YDB changes in no
// transfer, and a connection change of a transfer that is not paused.
func TestTransferChangeRefusal_FailurePath(t *testing.T) {
	retargeted := ingest()
	retargeted.Target = "other_log"
	remote := ingest()
	remote.Connection.ConnectionString = "grpc://primary:2136/?database=/prod"
	moved := remote
	moved.Connection.ConnectionString = "grpc://standby:2136/?database=/prod"
	tests := []struct {
		name    string
		desired ast.TransferSpec
		current ast.TransferSpec
		state   string
		want    string
	}{
		{name: "the target", desired: retargeted, current: ingest(), state: catalog.ReplicationPaused,
			want: "its target differ from the database's, and YDB changes none of them in place (`CONSUMER is not " +
				"supported in ALTER`, and ALTER takes no FROM or TO); drop it with DROP TRANSFER, which loses the " +
				"position of a consumer YDB created for it, and plan again"},
		{name: "a connection while running", desired: moved, current: remote, state: catalog.ReplicationRunning,
			want: "its connection or credential differs, and YDB changes them only while the transfer is paused " +
				"(`Modifications are not allowed in StandBy state`), which it is not (it is running); pause it with " +
				"ALTER TRANSFER `ingest` SET (STATE = 'PAUSED'), apply, and resume it with SET (STATE = 'StandBy')"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := ydbreplication.TransferChangeRefusal("ingest", test.desired, test.current, test.state)
			c.Assert(got, qt.IsNotNil)
			c.Assert(got.Subject, qt.Equals, "transfer ingest")
			c.Assert(got.Reason, qt.Equals, test.want)
		})
	}
}
