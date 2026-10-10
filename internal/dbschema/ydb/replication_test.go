package ydb_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_Replication"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/durationpb"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbreplication"
	ydbschema "ptah.run/internal/dbschema/ydb"
)

// replicaTableResult is a table an async replication writes, carrying the
// attribute YDB gives one.
func replicaTableResult() *Ydb_Table.DescribeTableResult {
	described := plainTable()
	described.Attributes = map[string]string{"__async_replica": "true"}
	return described
}

// replicationSource holds, as 26.2.1.14 describes them, a running replication
// of /prod at the root, a failed-over one in a directory, the replica table
// the first writes, a transfer of a topic in this database stored as 25.x
// stores one, and a transfer of another database's topic through a named
// lambda.
func replicationSource() fakeSource {
	return fakeSource{
		directories: map[string][]*Ydb_Scheme.Entry{
			"/local": {
				entry("mirror", Ydb_Scheme.Entry_REPLICATION),
				entry("ingest", Ydb_Scheme.Entry_TRANSFER),
				entry("order_log", Ydb_Scheme.Entry_TABLE),
				entry("replica", Ydb_Scheme.Entry_DIRECTORY),
				entry("dr", Ydb_Scheme.Entry_DIRECTORY),
			},
			"/local/replica": {entry("accounts", Ydb_Scheme.Entry_TABLE)},
			"/local/dr": {
				entry("failover", Ydb_Scheme.Entry_REPLICATION),
				entry("archive", Ydb_Scheme.Entry_TRANSFER),
				entry("ledger", Ydb_Scheme.Entry_TABLE),
			},
		},
		tables: map[string]*Ydb_Table.DescribeTableResult{
			"/local/order_log":        plainTable(),
			"/local/replica/accounts": replicaTableResult(),
			"/local/dr/ledger":        plainTable(),
		},
		replications: map[string]*Ydb_Replication.DescribeReplicationResult{
			"/local/mirror": {
				ConnectionParams: &Ydb_Replication.ConnectionParams{
					Endpoint: "primary:2136", Database: "/prod",
					ConnectionString: "grpc://primary:2136/?database=/prod",
					Credentials: &Ydb_Replication.ConnectionParams_Oauth{
						Oauth: &Ydb_Replication.ConnectionParams_OAuth{TokenSecretName: "token"}},
				},
				ConsistencyLevel: &Ydb_Replication.DescribeReplicationResult_GlobalConsistency{
					GlobalConsistency: &Ydb_Replication.ConsistencyLevelGlobal{
						CommitInterval: durationpb.New(1500 * time.Millisecond)}},
				Items: []*Ydb_Replication.DescribeReplicationResult_Item{
					{SourcePath: "/prod/accounts", DestinationPath: "/local/replica/accounts", Id: 1},
				},
				State: &Ydb_Replication.DescribeReplicationResult_Running{
					Running: &Ydb_Replication.DescribeReplicationResult_RunningState{}},
			},
			"/local/dr/failover": {
				ConnectionParams: &Ydb_Replication.ConnectionParams{
					Endpoint: "primary:2135", Database: "/prod", EnableSsl: true,
					Credentials: &Ydb_Replication.ConnectionParams_StaticCredentials_{
						StaticCredentials: &Ydb_Replication.ConnectionParams_StaticCredentials{
							User: "replicator", PasswordSecretName: "/local/secrets/password"}},
				},
				ConsistencyLevel: &Ydb_Replication.DescribeReplicationResult_GlobalConsistency{
					GlobalConsistency: &Ydb_Replication.ConsistencyLevelGlobal{
						CommitInterval: durationpb.New(10 * time.Second)}},
				Items: []*Ydb_Replication.DescribeReplicationResult_Item{
					{SourcePath: "/prod/ledger", DestinationPath: "/local/dr/ledger", Id: 1},
				},
				State: &Ydb_Replication.DescribeReplicationResult_Done{
					Done: &Ydb_Replication.DescribeReplicationResult_DoneState{}},
			},
		},
		transfers: map[string]*Ydb_Replication.DescribeTransferResult{
			"/local/ingest": {
				ConnectionParams:     &Ydb_Replication.ConnectionParams{ConnectionString: "grpc:///?database="},
				SourcePath:           "/orders/feed",
				DestinationPath:      "/local/order_log",
				TransformationLambda: "$__ydb_transfer_lambda = ($m) -> {\n  return []; -- none\n};\n",
				ConsumerName:         "fbc17198-8229c5ec-37e45ba-fe47b6c1",
				BatchSettings: &Ydb_Replication.DescribeTransferResult_BatchSettings{
					SizeBytes: new(uint64(8388608)), FlushInterval: durationpb.New(time.Minute)},
				State: &Ydb_Replication.DescribeTransferResult_Paused{
					Paused: &Ydb_Replication.DescribeTransferResult_PausedState{}},
			},
			"/local/dr/archive": {
				ConnectionParams: &Ydb_Replication.ConnectionParams{
					Endpoint: "primary:2136", Database: "/prod",
					Credentials: &Ydb_Replication.ConnectionParams_Oauth{
						Oauth: &Ydb_Replication.ConnectionParams_OAuth{TokenSecretName: "token"}},
				},
				SourcePath:           "/prod/events",
				DestinationPath:      "/local/dr/ledger",
				TransformationLambda: "$l = ($m) -> { return []; };\n$__ydb_transfer_lambda = $l;\n",
				ConsumerName:         "archive",
				BatchSettings: &Ydb_Replication.DescribeTransferResult_BatchSettings{
					SizeBytes: new(uint64(1048576)), FlushInterval: durationpb.New(10 * time.Second)},
				State: &Ydb_Replication.DescribeTransferResult_Error{
					Error: &Ydb_Replication.DescribeTransferResult_ErrorState{}},
			},
		},
	}
}

// A replication and a transfer are described by the settings a declaration
// names, each at YDB's default left out, with the state they report; a table a
// replication writes is recorded rather than described, so no plan drops or
// alters it.
func TestReader_DescribesReplications_HappyPath(t *testing.T) {
	c := qt.New(t)

	db := readFrom(c, replicationSource())

	replications, err := db.FeatureObjects.Select(func(ref objectidentity.ID) bool {
		return ref.Kind == objectidentity.Kind(ydbreplication.ReplicationKind) || ref.Kind == objectidentity.Kind(ydbreplication.TransferKind)
	}).All()
	c.Assert(err, qt.IsNil)
	c.Assert(replications, qt.DeepEquals, []schemaext.Object{
		ydbreplication.ObservedReplicationObject("", "mirror", ydbreplication.ReplicationSpec{
			Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod",
				TokenSecretName: "token"},
			Items:            []ydbreplication.Item{{Source: "accounts", Target: "replica/accounts"}},
			ConsistencyLevel: "global",
			CommitInterval:   "PT1.5S",
		}, ydbreplication.StateRunning),
		ydbreplication.ObservedReplicationObject("dr", "failover", ydbreplication.ReplicationSpec{
			Connection: ydbreplication.Connection{ConnectionString: "grpcs://primary:2135/?database=/prod",
				User: "replicator", PasswordSecretPath: "secrets/password"},
			Items:            []ydbreplication.Item{{Source: "ledger", Target: "dr/ledger"}},
			ConsistencyLevel: "global",
		}, ydbreplication.StateDone),
		ydbreplication.ObservedTransferObject("", "ingest", ydbreplication.TransferSpec{
			Source: "orders/feed", Target: "order_log", Lambda: "($m) -> {\n  return []; -- none\n}",
			Consumer: "fbc17198-8229c5ec-37e45ba-fe47b6c1",
		}, ydbreplication.StatePaused),
		ydbreplication.ObservedTransferObject("dr", "archive", ydbreplication.TransferSpec{
			Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod",
				TokenSecretName: "token"},
			Source: "events", Target: "dr/ledger",
			Lambda:         "$l = ($m) -> { return []; };\n$__ydb_transfer_lambda = $l;\n",
			Consumer:       "archive",
			BatchSizeBytes: 1048576, FlushInterval: "PT10S",
		}, ydbreplication.StateError),
	})
	c.Assert(db.Tables, qt.HasLen, 2)
	c.Assert(db.NotDescribed, qt.DeepEquals, coverage.Set{}.With(
		coverage.Object{Kind: coverage.ReplicaTable, Name: "replica.accounts", Reason: coverage.Unsupported,
			Provenance: coverage.Observed},
	))
}

// A line without the key has a replication or a transfer recorded rather
// than described: 25.1 creates no transfer unless a feature flag is set, and a
// transfer read there is one the renderer would refuse to write.
func TestReader_RecordsAFamilyTheLineLacks(t *testing.T) {
	c := qt.New(t)

	db, err := ydbschema.NewReaderFromSource(replicationSource(), "/local", capability.YDB251()).
		ReadSchemaContext(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureObjects.Select(func(ref objectidentity.ID) bool {
		return ref.Kind == objectidentity.Kind(ydbreplication.ReplicationKind)
	}).Len(), qt.Equals, 2)
	c.Assert(db.FeatureObjects.Select(func(ref objectidentity.ID) bool {
		return ref.Kind == objectidentity.Kind(ydbreplication.TransferKind)
	}).Len(), qt.Equals, 0)
	for _, ref := range []objectidentity.ID{ydbreplication.TransferRef("", "ingest"), ydbreplication.TransferRef("dr", "archive")} {
		c.Assert(db.FeatureCoverage.Lookup(ydbreplication.TransferKind, ref), qt.DeepEquals,
			schemaext.Knowledge{State: schemaext.Uninspected, Reason: ydbreplication.UnsupportedTransferReason})
	}
}

// A description Ptah cannot read whole fails the read, naming what it could
// not read, rather than reading as a replication without it.
func TestReader_DescribesReplications_FailurePath(t *testing.T) {
	withUnknown := &Ydb_Replication.DescribeReplicationResult{
		ConnectionParams: &Ydb_Replication.ConnectionParams{Endpoint: "primary:2136", Database: "/prod"},
	}
	withUnknown.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1))
	microseconds := &Ydb_Replication.DescribeReplicationResult{
		ConnectionParams: &Ydb_Replication.ConnectionParams{Endpoint: "primary:2136", Database: "/prod"},
		ConsistencyLevel: &Ydb_Replication.DescribeReplicationResult_GlobalConsistency{
			GlobalConsistency: &Ydb_Replication.ConsistencyLevelGlobal{
				CommitInterval: durationpb.New(1500 * time.Microsecond)}},
	}
	halfSecond := &Ydb_Replication.DescribeTransferResult{
		SourcePath: "tp", DestinationPath: "/local/t", TransformationLambda: "$__ydb_transfer_lambda = 1;\n",
		BatchSettings: &Ydb_Replication.DescribeTransferResult_BatchSettings{
			FlushInterval: durationpb.New(1500 * time.Millisecond)},
	}
	tests := []struct {
		name         string
		kind         Ydb_Scheme.Entry_Type
		replications map[string]*Ydb_Replication.DescribeReplicationResult
		transfers    map[string]*Ydb_Replication.DescribeTransferResult
		wantErr      string
	}{
		{name: "an unknown field", kind: Ydb_Scheme.Entry_REPLICATION, replications: map[string]*Ydb_Replication.DescribeReplicationResult{
			"/local/r": withUnknown}, wantErr: `YDB async replication /local/r: its description holds fields 99, ` +
			`which Ptah does not read`},
		{name: "a commit interval of microseconds", kind: Ydb_Scheme.Entry_REPLICATION, replications: map[string]*Ydb_Replication.DescribeReplicationResult{
			"/local/r": microseconds}, wantErr: `YDB async replication /local/r: its commit interval holds a fraction ` +
			`of a millisecond, which no declaration Ptah reads can name`},
		{name: "a flush interval of a second and a half", kind: Ydb_Scheme.Entry_TRANSFER, transfers: map[string]*Ydb_Replication.DescribeTransferResult{
			"/local/r": halfSecond}, wantErr: `YDB transfer /local/r: its flush interval holds a fraction of a ` +
			`second, which no declaration Ptah reads can name`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := fakeSource{
				directories:  map[string][]*Ydb_Scheme.Entry{"/local": {entry("r", test.kind)}},
				replications: test.replications,
				transfers:    test.transfers,
			}

			db, err := ydbschema.NewReaderFromSource(source, "/local", capability.YDB262()).
				ReadSchemaContext(context.Background())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
