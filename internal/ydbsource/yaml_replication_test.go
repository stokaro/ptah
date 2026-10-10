package ydbsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/ydb/ydbreplication"
)

// TestParse_YDBAsyncReplication_HappyPath reads async replications and
// transfers keyed by name, in key order, each item in the order written and
// each setting under the key the annotation reads.
func TestParse_YDBAsyncReplication_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse(ydbYAMLOwners, []byte(`
async_replications:
  mirror:
    schema: /replicas/
    connection_string: grpcs://primary:2135/?database=/prod
    user: replicator
    password_secret_name: password
    consistency_level: GLOBAL
    commit_interval: PT30S
    items:
      - source: accounts
        target: replica/accounts
      - source: /prod/ledger
        target: replica/ledger
transfers:
  ingest:
    source: orders/feed
    target: order_log
    using: "($m) -> { return []; }"
    batch_size_bytes: 1048576
  archive:
    name: archive_transfer
    connection_string: grpc://primary:2136/?database=/prod
    token_secret_name: token
    source: events
    target: archive
    using: "($m) -> { return []; }"
    consumer: archive
    flush_interval: PT10S
`))

	c.Assert(err, qt.IsNil)
	objects, err := db.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.DeepEquals, []schemaext.Object{
		ydbreplication.DesiredReplicationObject("replicas", "mirror", "", ydbreplication.ReplicationSpec{
			Connection: ydbreplication.Connection{ConnectionString: "grpcs://primary:2135/?database=/prod",
				User: "replicator", PasswordSecretName: "password"},
			Items: []ydbreplication.Item{
				{Source: "accounts", Target: "replica/accounts"},
				{Source: "/prod/ledger", Target: "replica/ledger"},
			},
			ConsistencyLevel: "global",
			CommitInterval:   "PT30S",
		}),
		ydbreplication.DesiredTransferObject("", "archive_transfer", "", ydbreplication.TransferSpec{
			Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod",
				TokenSecretName: "token"},
			Source: "events", Target: "archive", Lambda: "($m) -> { return []; }", Consumer: "archive",
			FlushInterval: "PT10S",
		}),
		ydbreplication.DesiredTransferObject("", "ingest", "", ydbreplication.TransferSpec{Source: "orders/feed", Target: "order_log",
			Lambda: "($m) -> { return []; }", BatchSizeBytes: 1048576}),
	})
}

// TestParse_YDBAsyncReplication_FailurePath refuses a value YDB would refuse,
// an empty one included, a replication with no item and a key neither family
// has.
func TestParse_YDBAsyncReplication_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		document string
		wantErr  string
	}{
		{
			name: "no item",
			document: `
async_replications:
  mirror:
    connection_string: grpc://primary:2136/?database=/prod
`,
			wantErr: `async replication "mirror": declares no item; list the tables it replicates under items`,
		},
		{
			name: "no connection",
			document: `
async_replications:
  mirror:
    token_secret_name: token
    items:
      - source: a
        target: ra
`,
			wantErr: `async replication "mirror": invalid connection_string: a replication reads another database, .*`,
		},
		{
			name: "an empty credential",
			document: `
async_replications:
  mirror:
    connection_string: grpc://primary:2136/?database=/prod
    token_secret_name: ""
    items:
      - source: a
        target: ra
`,
			wantErr: `async replication "mirror": invalid token_secret_name: names nothing; leave the attribute out instead`,
		},
		{
			name: "an absolute target",
			document: `
async_replications:
  mirror:
    connection_string: grpc://primary:2136/?database=/prod
    items:
      - source: a
        target: /prod/ra
`,
			wantErr: `async replication "mirror", item 1: invalid target "/prod/ra": takes a path relative to .*`,
		},
		{
			name: "an unknown key",
			document: `
async_replications:
  mirror:
    connection_string: grpc://primary:2136/?database=/prod
    ca_cert: x
    items:
      - source: a
        target: ra
`,
			wantErr: `(?s).*field ca_cert not found.*`,
		},
		{
			name: "a transfer flushing every half second",
			document: `
transfers:
  ingest:
    source: tp
    target: t
    using: "($m) -> { return []; }"
    flush_interval: PT0.5S
`,
			wantErr: `transfer "ingest": invalid flush_interval "PT0.5S": YDB keeps whole seconds, .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := yamlschema.Parse(ydbYAMLOwners, []byte(test.document))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
