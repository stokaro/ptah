package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/internal/sqlschema"
)

// declared lists the objects of kind database declares.
func declared(c *qt.C, database schemamodel.Database, kind schemaext.Kind) []schemaext.Object {
	c.Helper()
	objects, err := database.FeatureObjects.Select(func(ref objectidentity.ID) bool {
		return ref.Kind == objectidentity.Kind(kind)
	}).All()
	c.Assert(err, qt.IsNil)
	return objects
}

// declaredConnection is the connection of the one replication database
// declares.
func declaredConnection(c *qt.C, database schemamodel.Database) ydbreplication.Connection {
	c.Helper()
	replications := declared(c, database, ydbreplication.ReplicationKind)
	c.Assert(replications, qt.HasLen, 1)
	value, ok := replications[0].Value.(*ydbreplication.DesiredReplication)
	c.Assert(ok, qt.IsTrue)
	return value.Spec.Connection
}

const yqlReplication = "CREATE ASYNC REPLICATION `archive/mirror` FOR `/remote/source` AS `archive/copy` WITH (CONNECTION_STRING='grpc://source:2136/?database=/remote');"
const yqlTransferLambda = "($msg) -> { $items = [<|id:$msg._offset|>]; RETURN $items; }"
const yqlTransfer = "CREATE TRANSFER `archive/ingest` FROM `archive/events` TO `archive/rows` USING " + yqlTransferLambda + ";"

func TestReadYQLReplication(t *testing.T) {
	c := qt.New(t)
	source := "CREATE ASYNC REPLICATION `archive/mirror` FOR `/remote/source` AS `archive/copy`, other AS `archive/other` WITH (ENDPOINT='grpcs://source:2135', DATABASE='/remote', CONSISTENCY_LEVEL='GLOBAL', COMMIT_INTERVAL=Interval('PT0.5S'), TOKEN_SECRET_NAME='token');"
	database, _, err := sqlschema.Read([]byte(source), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(declared(c, database, ydbreplication.ReplicationKind), qt.DeepEquals, []schemaext.Object{
		ydbreplication.DesiredReplicationObject("archive", "mirror", "", ydbreplication.ReplicationSpec{
			Connection: ydbreplication.Connection{ConnectionString: "grpcs://source:2135/?database=/remote", TokenSecretName: "token"},
			Items:      []ydbreplication.Item{{Source: "/remote/source", Target: "archive/copy"}, {Source: "other", Target: "archive/other"}}, ConsistencyLevel: "global", CommitInterval: "PT0.5S",
		})})
	c.Assert(database.FeatureCoverage.Lookup(ydbreplication.ReplicationKind, ydbreplication.ReplicationRef("", "undeclared")).State, qt.Equals, schemaext.Complete)
}

func TestReadYQLTransfer(t *testing.T) {
	c := qt.New(t)
	source := "CREATE TRANSFER `archive/ingest` FROM `archive/events` TO `archive/rows` USING " + yqlTransferLambda + " WITH (BATCH_SIZE_BYTES=4096,FLUSH_INTERVAL=Interval('PT2S'),CONSUMER='events_reader');"
	database, _, err := sqlschema.Read([]byte(source), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(declared(c, database, ydbreplication.TransferKind), qt.DeepEquals, []schemaext.Object{
		ydbreplication.DesiredTransferObject("archive", "ingest", "", ydbreplication.TransferSpec{
			Source: "archive/events", Target: "archive/rows", Lambda: yqlTransferLambda, BatchSizeBytes: 4096, FlushInterval: "PT2S", Consumer: "events_reader",
		})})
	c.Assert(database.FeatureCoverage.Lookup(ydbreplication.TransferKind, ydbreplication.TransferRef("", "undeclared")).State, qt.Equals, schemaext.Complete)
}

func TestReadYQLReplicationChanges(t *testing.T) {
	c := qt.New(t)
	source := yqlReplication + "ALTER ASYNC REPLICATION `archive/mirror` SET (ENDPOINT='grpcs://moved:2135'); ALTER ASYNC REPLICATION `archive/mirror` SET (DATABASE='/elsewhere', USER='reader', PASSWORD_SECRET_NAME='credential');"
	database, _, err := sqlschema.Read([]byte(source), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(declared(c, database, ydbreplication.ReplicationKind), qt.DeepEquals, []schemaext.Object{
		ydbreplication.DesiredReplicationObject("archive", "mirror", "", ydbreplication.ReplicationSpec{
			Connection: ydbreplication.Connection{ConnectionString: "grpcs://moved:2135/?database=/elsewhere", User: "reader", PasswordSecretName: "credential"},
			Items:      []ydbreplication.Item{{Source: "/remote/source", Target: "archive/copy"}},
		})})
}

func TestReadYQLTransferChanges(t *testing.T) {
	c := qt.New(t)
	const changed = "($msg) -> { RETURN [<|id:$msg._offset+1|>]; }"
	source := yqlTransfer + "ALTER TRANSFER `archive/ingest` SET USING " + changed + ", SET (BATCH_SIZE_BYTES=8192,FLUSH_INTERVAL=Interval('PT3S'));"
	database, _, err := sqlschema.Read([]byte(source), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(declared(c, database, ydbreplication.TransferKind), qt.DeepEquals, []schemaext.Object{
		ydbreplication.DesiredTransferObject("archive", "ingest", "", ydbreplication.TransferSpec{Source: "archive/events", Target: "archive/rows", Lambda: changed, BatchSizeBytes: 8192, FlushInterval: "PT3S"})})
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
			c.Assert(database.FeatureObjects.Len(), qt.Equals, 0)
		})
	}
}

// Both server lines preserve the user on password rotation and replace the
// credential variant on a switch between password and token references.
func TestReadYQLCredentialChanges(t *testing.T) {
	for _, tc := range []struct {
		name    string
		changes string
		want    ydbreplication.Connection
	}{
		{name: "password rotation", changes: "USER='reader',PASSWORD_SECRET_NAME='first'); ALTER ASYNC REPLICATION `archive/mirror` SET (PASSWORD_SECRET_NAME='second'", want: ydbreplication.Connection{User: "reader", PasswordSecretName: "second"}},
		{name: "password to token", changes: "USER='reader',PASSWORD_SECRET_NAME='first'); ALTER ASYNC REPLICATION `archive/mirror` SET (TOKEN_SECRET_NAME='token'", want: ydbreplication.Connection{TokenSecretName: "token"}},
		{name: "token to password", changes: "TOKEN_SECRET_NAME='token'); ALTER ASYNC REPLICATION `archive/mirror` SET (USER='reader',PASSWORD_SECRET_NAME='password'", want: ydbreplication.Connection{User: "reader", PasswordSecretName: "password"}},
		{name: "secret name to path", changes: "TOKEN_SECRET_NAME='token'); ALTER ASYNC REPLICATION `archive/mirror` SET (TOKEN_SECRET_PATH='secrets/token'", want: ydbreplication.Connection{TokenSecretPath: "secrets/token"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			source := yqlReplication + "ALTER ASYNC REPLICATION `archive/mirror` SET (" + tc.changes + ");"
			database, _, err := sqlschema.Read([]byte(source), "ydb")
			c.Assert(err, qt.IsNil)
			tc.want.ConnectionString = "grpc://source:2136/?database=/remote"
			c.Assert(declaredConnection(c, database), qt.DeepEquals, tc.want)
		})
	}
}
