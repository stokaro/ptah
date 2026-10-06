package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

const yqlReplication = "CREATE ASYNC REPLICATION `archive/mirror` FOR `/remote/source` AS `archive/copy` WITH (CONNECTION_STRING='grpc://source:2136/?database=/remote');"
const yqlTransferLambda = "($msg) -> { $items = [<|id:$msg._offset|>]; RETURN $items; }"
const yqlTransfer = "CREATE TRANSFER `archive/ingest` FROM `archive/events` TO `archive/rows` USING " + yqlTransferLambda + ";"

func TestReadYQLReplication(t *testing.T) {
	c := qt.New(t)
	source := "CREATE ASYNC REPLICATION `archive/mirror` FOR `/remote/source` AS `archive/copy`, other AS `archive/other` WITH (ENDPOINT='grpcs://source:2135', DATABASE='/remote', CONSISTENCY_LEVEL='GLOBAL', COMMIT_INTERVAL=Interval('PT0.5S'), TOKEN_SECRET_NAME='token');"
	database, _, err := sqlschema.Read([]byte(source), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(database.AsyncReplications, qt.DeepEquals, []schemamodel.AsyncReplication{{Name: "mirror", Schema: "archive", Spec: ast.AsyncReplicationSpec{
		Connection: ast.ReplicationConnectionSpec{ConnectionString: "grpcs://source:2135/?database=/remote", TokenSecretName: "token"},
		Items:      []ast.AsyncReplicationItem{{Source: "/remote/source", Target: "archive/copy"}, {Source: "other", Target: "archive/other"}}, ConsistencyLevel: "global", CommitInterval: "PT0.5S",
	}}})
	c.Assert(database.NotDescribed.Describes(coverage.Replication), qt.IsTrue)
}

func TestReadYQLTransfer(t *testing.T) {
	c := qt.New(t)
	source := "CREATE TRANSFER `archive/ingest` FROM `archive/events` TO `archive/rows` USING " + yqlTransferLambda + " WITH (BATCH_SIZE_BYTES=4096,FLUSH_INTERVAL=Interval('PT2S'),CONSUMER='events_reader');"
	database, _, err := sqlschema.Read([]byte(source), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Transfers, qt.DeepEquals, []schemamodel.Transfer{{Name: "ingest", Schema: "archive", Spec: ast.TransferSpec{
		Source: "archive/events", Target: "archive/rows", Lambda: yqlTransferLambda, BatchSizeBytes: 4096, FlushInterval: "PT2S", Consumer: "events_reader",
	}}})
	c.Assert(database.NotDescribed.Describes(coverage.Transfer), qt.IsTrue)
}

func TestReadYQLReplicationChanges(t *testing.T) {
	c := qt.New(t)
	source := yqlReplication + "ALTER ASYNC REPLICATION `archive/mirror` SET (ENDPOINT='grpcs://moved:2135'); ALTER ASYNC REPLICATION `archive/mirror` SET (DATABASE='/elsewhere', USER='reader', PASSWORD_SECRET_NAME='credential');"
	database, _, err := sqlschema.Read([]byte(source), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(database.AsyncReplications, qt.HasLen, 1)
	c.Assert(database.AsyncReplications[0].Spec.Connection, qt.DeepEquals, ast.ReplicationConnectionSpec{ConnectionString: "grpcs://moved:2135/?database=/elsewhere", User: "reader", PasswordSecretName: "credential"})
	c.Assert(database.AsyncReplications[0].Spec.Items, qt.DeepEquals, []ast.AsyncReplicationItem{{Source: "/remote/source", Target: "archive/copy"}})
}

func TestReadYQLTransferChanges(t *testing.T) {
	c := qt.New(t)
	const changed = "($msg) -> { RETURN [<|id:$msg._offset+1|>]; }"
	source := yqlTransfer + "ALTER TRANSFER `archive/ingest` SET USING " + changed + ", SET (BATCH_SIZE_BYTES=8192,FLUSH_INTERVAL=Interval('PT3S'));"
	database, _, err := sqlschema.Read([]byte(source), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Transfers, qt.DeepEquals, []schemamodel.Transfer{{Name: "ingest", Schema: "archive", Spec: ast.TransferSpec{Source: "archive/events", Target: "archive/rows", Lambda: changed, BatchSizeBytes: 8192, FlushInterval: "PT3S"}}})
}

func TestYQLReplicationRefusalsHideValues(t *testing.T) {
	for _, source := range []string{
		"CREATE ASYNC REPLICATION x FOR src AS dst WITH (TOKEN='SENTINEL');",
		"CREATE ASYNC REPLICATION x FOR src AS dst WITH (CONNECTION_STRING='grpc://user:SENTINEL@source:2136/?database=/remote');",
		"CREATE ASYNC REPLICATION x FOR src AS dst WITH (ENDPOINT='SENTINEL');",
		"CREATE ASYNC REPLICATION x FOR src AS `/absolute` WITH (CONNECTION_STRING='SENTINEL');",
		"CREATE ASYNC REPLICATION x FOR src AS dst WITH (CONNECTION_STRING='SENTINEL',CA_CERT='SENTINEL');",
		"CREATE ASYNC REPLICATION x FOR src AS dst WITH (CONNECTION_STRING='SENTINEL') 'SENTINEL';",
		"CREATE TRANSFER x FROM src TO dst USING $SENTINEL;",
		"CREATE TRANSFER x FROM src TO dst USING ($msg) -> { RETURN [<|id:1|>]; WITH (TOKEN='SENTINEL');",
		"CREATE TRANSFER x FROM src TO dst USING " + yqlTransferLambda + " WITH (BATCH_SIZE_BYTES=0);",
		"CREATE TRANSFER x FROM src TO dst USING " + yqlTransferLambda + " WITH (FLUSH_INTERVAL=Interval('PT0.5S'));",
		"CREATE TRANSFER x FROM src TO dst USING " + yqlTransferLambda + " WITH (CONSUMER='one',CONSUMER='SENTINEL');",
		yqlReplication + "ALTER ASYNC REPLICATION `archive/mirror` SET (CONSISTENCY_LEVEL='GLOBAL');",
		yqlReplication + "ALTER ASYNC REPLICATION `archive/mirror` SET (STATE='PAUSED');",
		yqlTransfer + "ALTER TRANSFER `archive/ingest` SET (CONSUMER='SENTINEL');",
		yqlTransfer + "ALTER TRANSFER `archive/ingest` SET (PASSWORD='SENTINEL');",
		yqlTransfer + "ALTER TRANSFER `archive/ingest` SET (FLUSH_INTERVAL=Interval('PT2S')), SET (FLUSH_INTERVAL=Interval('PT3S'));",
		"ALTER TRANSFER missing SET USING " + yqlTransferLambda + ";",
		"ALTER ASYNC REPLICATION missing SET (DATABASE='/remote');",
		"CREATE ASYNC REPLICATION x FOR src AS dst, other AS dst WITH (CONNECTION_STRING='grpc://source:2136/?database=/remote');",
		"CREATE ASYNC REPLICATION x FOR src AS dst WITH (ENDPOINT='source:2136',DATABASE='/remote',CONNECTION_STRING='grpc://source:2136/?database=/remote');",
		"CREATE ASYNC REPLICATION x FOR src AS dst WITH (CONNECTION_STRING='grpc://source:2136/?database=/remote',USER=' reader',PASSWORD_SECRET_NAME='password');",
		yqlReplication + "ALTER ASYNC REPLICATION `archive/mirror` SET (TOKEN_SECRET_NAME='token',USER='reader',PASSWORD_SECRET_NAME='password');",
		"CREATE ASYNC REPLICATION x FOR ` src` AS dst WITH (CONNECTION_STRING='grpc://source:2136/?database=/remote');",
		"CREATE TRANSFER x FROM src TO ` dst` USING " + yqlTransferLambda + ";",
		"CREATE ASYNC REPLICATION x FOR src AS dst WITH (ENDPOINT='grpc://grpcs://source:2136',DATABASE='/remote');",
		"CREATE TRANSFER x FROM src TO dst USING " + yqlTransferLambda + " WITH (FLUSH_INTERVAL=Interval(' PT2S'));",
		yqlReplication + yqlReplication,
		yqlTransfer + yqlTransfer,
	} {
		t.Run(source, func(t *testing.T) {
			c := qt.New(t)
			database, statements, err := sqlschema.Read([]byte(source), "ydb")
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Not(qt.Contains), "SENTINEL")
			c.Assert(statements, qt.IsNil)
			c.Assert(database.AsyncReplications, qt.HasLen, 0)
			c.Assert(database.Transfers, qt.HasLen, 0)
		})
	}
}

// Both server lines preserve the user on password rotation and replace the
// credential variant on a switch between password and token references.
func TestReadYQLCredentialChanges(t *testing.T) {
	for _, tc := range []struct {
		name    string
		changes string
		want    ast.ReplicationConnectionSpec
	}{
		{name: "password rotation", changes: "USER='reader',PASSWORD_SECRET_NAME='first'); ALTER ASYNC REPLICATION `archive/mirror` SET (PASSWORD_SECRET_NAME='second'", want: ast.ReplicationConnectionSpec{User: "reader", PasswordSecretName: "second"}},
		{name: "password to token", changes: "USER='reader',PASSWORD_SECRET_NAME='first'); ALTER ASYNC REPLICATION `archive/mirror` SET (TOKEN_SECRET_NAME='token'", want: ast.ReplicationConnectionSpec{TokenSecretName: "token"}},
		{name: "token to password", changes: "TOKEN_SECRET_NAME='token'); ALTER ASYNC REPLICATION `archive/mirror` SET (USER='reader',PASSWORD_SECRET_NAME='password'", want: ast.ReplicationConnectionSpec{User: "reader", PasswordSecretName: "password"}},
		{name: "secret name to path", changes: "TOKEN_SECRET_NAME='token'); ALTER ASYNC REPLICATION `archive/mirror` SET (TOKEN_SECRET_PATH='secrets/token'", want: ast.ReplicationConnectionSpec{TokenSecretPath: "secrets/token"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			source := yqlReplication + "ALTER ASYNC REPLICATION `archive/mirror` SET (" + tc.changes + ");"
			database, _, err := sqlschema.Read([]byte(source), "ydb")
			c.Assert(err, qt.IsNil)
			c.Assert(database.AsyncReplications, qt.HasLen, 1)
			tc.want.ConnectionString = "grpc://source:2136/?database=/remote"
			c.Assert(database.AsyncReplications[0].Spec.Connection, qt.DeepEquals, tc.want)
		})
	}
}
