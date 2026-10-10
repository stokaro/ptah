package ydbreplication_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/ydb/ydbreplication"
)

// TestCreateReplicationStatement writes each item in the order declared and
// each setting the declaration names, the level in YDB's upper case and the
// commit interval as the milliseconds YDB keeps.
func TestCreateReplicationStatement(t *testing.T) {
	tests := []struct {
		name string
		repl string
		spec ydbreplication.ReplicationSpec
		want string
	}{
		{
			name: "a token secret",
			repl: "mirror",
			spec: mirror(),
			want: "CREATE ASYNC REPLICATION `mirror` FOR `accounts` AS `replica/accounts` WITH (" +
				"CONNECTION_STRING = 'grpc://primary:2136/?database=/prod', TOKEN_SECRET_NAME = 'token');",
		},
		{
			name: "a user at the global level, in a directory",
			repl: "replicas/mirror",
			spec: ydbreplication.ReplicationSpec{
				Connection: ydbreplication.Connection{ConnectionString: "grpcs://primary:2135/?database=/prod",
					User: "o'neil", PasswordSecretPath: "secrets/password"},
				Items:            []ydbreplication.Item{{Source: "/prod/b", Target: "rb"}, {Source: "a", Target: "ra"}},
				ConsistencyLevel: "global", CommitInterval: "PT60.5S",
			},
			want: "CREATE ASYNC REPLICATION `replicas/mirror` FOR `/prod/b` AS `rb`, `a` AS `ra` WITH (" +
				"CONNECTION_STRING = 'grpcs://primary:2135/?database=/prod', USER = 'o\\'neil', " +
				"PASSWORD_SECRET_PATH = 'secrets/password', CONSISTENCY_LEVEL = 'GLOBAL', " +
				"COMMIT_INTERVAL = Interval('PT1M0.5S'));",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbreplication.CreateReplicationStatement(test.repl, test.spec), qt.Equals, test.want)
		})
	}
}

// TestAlterReplicationStatement names the connection string when it differs
// and the whole credential when any part of it does, and writes nothing when
// neither differs.
func TestAlterReplicationStatement(t *testing.T) {
	moved := mirror()
	moved.Connection.ConnectionString = "grpcs://standby:2135/?database=/prod"
	byUser := mirror()
	byUser.Connection.TokenSecretName, byUser.Connection.User, byUser.Connection.PasswordSecretName =
		"", "replicator", "password"
	rotated := byUser
	rotated.Connection.PasswordSecretName = "password2"
	tests := []struct {
		name     string
		desired  ydbreplication.ReplicationSpec
		previous ydbreplication.ReplicationSpec
		want     string
	}{
		{name: "nothing", desired: mirror(), previous: mirror(), want: ""},
		{name: "the host", desired: moved, previous: mirror(),
			want: "ALTER ASYNC REPLICATION `mirror` SET (CONNECTION_STRING = 'grpcs://standby:2135/?database=/prod');"},
		{name: "a token for a password", desired: byUser, previous: mirror(),
			want: "ALTER ASYNC REPLICATION `mirror` SET (USER = 'replicator', PASSWORD_SECRET_NAME = 'password');"},
		{name: "the password secret alone", desired: rotated, previous: byUser,
			want: "ALTER ASYNC REPLICATION `mirror` SET (USER = 'replicator', PASSWORD_SECRET_NAME = 'password2');"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbreplication.AlterReplicationStatement("mirror", test.desired, test.previous), qt.Equals,
				test.want)
		})
	}
}

// TestDropReplicationStatement writes CASCADE only when asked.
func TestDropReplicationStatement(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbreplication.DropReplicationStatement("replicas/mirror", false), qt.Equals,
		"DROP ASYNC REPLICATION `replicas/mirror`;")
	c.Assert(ydbreplication.DropReplicationStatement("mirror", true), qt.Equals,
		"DROP ASYNC REPLICATION `mirror` CASCADE;")
}

// TestCreateTransferStatement writes the lambda as declared and only the
// settings the declaration names.
func TestCreateTransferStatement(t *testing.T) {
	full := ingest()
	full.Consumer, full.BatchSizeBytes, full.FlushInterval = "ptah", 1048576, "PT30S"
	full.Connection = ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod",
		TokenSecretName: "token"}
	tests := []struct {
		name string
		spec ydbreplication.TransferSpec
		want string
	}{
		{name: "a local topic", spec: ingest(),
			want: "CREATE TRANSFER `ingest` FROM `orders/feed` TO `order_log` USING ($m) -> { return []; };"},
		{name: "every setting", spec: full,
			want: "CREATE TRANSFER `ingest` FROM `orders/feed` TO `order_log` USING ($m) -> { return []; } WITH (" +
				"CONNECTION_STRING = 'grpc://primary:2136/?database=/prod', TOKEN_SECRET_NAME = 'token', " +
				"CONSUMER = 'ptah', BATCH_SIZE_BYTES = 1048576, FLUSH_INTERVAL = Interval('PT30S'));"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbreplication.CreateTransferStatement("ingest", test.spec), qt.Equals, test.want)
		})
	}
}

// TestAlterTransferStatement writes the lambda and both batch settings in one
// statement, each where it differs, and nothing where nothing it can change
// differs.
func TestAlterTransferStatement(t *testing.T) {
	relambda := ingest()
	relambda.Lambda = "($m) -> { return [<| id: 1 |>]; }"
	both := relambda
	both.FlushInterval = "PT10S"
	consumer := ingest()
	consumer.Consumer = "other"
	tests := []struct {
		name    string
		desired ydbreplication.TransferSpec
		want    string
	}{
		{name: "nothing", desired: ingest(), want: ""},
		{name: "a consumer, which it cannot change", desired: consumer, want: ""},
		{name: "the lambda", desired: relambda,
			want: "ALTER TRANSFER `ingest` SET USING ($m) -> { return [<| id: 1 |>]; };"},
		{name: "the lambda and the flush interval", desired: both,
			want: "ALTER TRANSFER `ingest` SET USING ($m) -> { return [<| id: 1 |>]; }, " +
				"SET (BATCH_SIZE_BYTES = 8388608, FLUSH_INTERVAL = Interval('PT10S'));"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbreplication.AlterTransferStatement("ingest", test.desired, ingest()), qt.Equals, test.want)
		})
	}
	t.Run("drop", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(ydbreplication.DropTransferStatement("etl/ingest"), qt.Equals, "DROP TRANSFER `etl/ingest`;")
	})
}
