package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbreplication"
)

// replicationSource is an entity file whose Replicas struct carries the given
// annotations.
func replicationSource(annotations string) string {
	return `package entities

` + annotations + `
type Replicas struct{}
`
}

// TestParseSource_AsyncReplication_HappyPath reads a replication with the
// items written for it, an item written above its replication included, and a
// transfer.
func TestParseSource_AsyncReplication_HappyPath(t *testing.T) {
	c := qt.New(t)
	db, err := goschema.ParseSource(noOwners, "replicas.go", replicationSource(
		`//ptah:schema:async_replication:item replication="mirror" schema="replicas" source="/prod/ledger" target="replica/ledger"
//ptah:schema:async_replication name="mirror" schema="/replicas/" connection_string="grpc://primary:2136/?database=/prod" token_secret_name="token" consistency_level="GLOBAL"
//ptah:schema:async_replication:item replication="mirror" schema="replicas" source="accounts" target="replica/accounts"
//ptah:schema:transfer name="ingest" source="orders/feed" target="order_log" using="($m) -> { return []; }" consumer="ptah" flush_interval="PT10S"`))
	c.Assert(err, qt.IsNil)
	objects, err := db.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.DeepEquals, []schemaext.Object{
		ydbreplication.DesiredReplicationObject("replicas", "mirror", "Replicas", ydbreplication.ReplicationSpec{
			Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod",
				TokenSecretName: "token"},
			Items: []ydbreplication.Item{
				{Source: "/prod/ledger", Target: "replica/ledger"},
				{Source: "accounts", Target: "replica/accounts"},
			},
			ConsistencyLevel: "global",
		}),
		ydbreplication.DesiredTransferObject("", "ingest", "Replicas", ydbreplication.TransferSpec{Source: "orders/feed",
			Target: "order_log", Lambda: "($m) -> { return []; }", Consumer: "ptah", FlushInterval: "PT10S"}),
	})
}

// TestParseSource_AsyncReplication_FailurePath refuses a declaration with no
// effect, or one YDB would refuse or keep differently, where it was written.
func TestParseSource_AsyncReplication_FailurePath(t *testing.T) {
	const replication = `//ptah:schema:async_replication name="mirror" connection_string="grpc://primary:2136/?database=/prod"`
	tests := []struct {
		name        string
		annotations string
		wantErr     string
	}{
		{name: "a replication with no item", annotations: replication,
			wantErr: `async replication "mirror" declares no item; declare the tables it replicates with ` +
				`//ptah:schema:async_replication:item in the same file`},
		{name: "an item of no replication",
			annotations: `//ptah:schema:async_replication:item replication="other" source="a" target="ra"`,
			wantErr:     `the file declares no async replication "other" for the item replicating "a" on .* at Replicas`},
		{name: "an item of the replication in another directory",
			annotations: replication + "\n" + `//ptah:schema:async_replication:item replication="mirror" schema="replicas" source="a" target="ra"`,
			wantErr:     `the file declares no async replication "replicas.mirror" for the item replicating "a" .*`},
		{name: "two items creating one replica",
			annotations: replication + "\n" + `//ptah:schema:async_replication:item replication="mirror" source="a" target="ra"
//ptah:schema:async_replication:item replication="mirror" source="b" target="ra"`,
			wantErr: `async replication "mirror" declares two items with target "ra" on .* at Replicas`},
		{name: "no connection",
			annotations: `//ptah:schema:async_replication name="mirror" token_secret_name="t"`,
			wantErr:     `missing required annotation attribute "connection_string" on //ptah:schema:async_replication at Replicas`},
		{name: "an unknown attribute",
			annotations: `//ptah:schema:async_replication name="mirror" connection_string="grpc://p:2136/?database=/prod" ca_cert="x"`,
			wantErr:     `unknown annotation attribute "ca_cert" on //ptah:schema:async_replication at Replicas.*`},
		{name: "a password in the connection string",
			annotations: `//ptah:schema:async_replication name="mirror" connection_string="grpc://p:2136/?database=/prod&password=x"`,
			wantErr:     `invalid connection_string .* on //ptah:schema:async_replication at Replicas`},
		{name: "an absolute target",
			annotations: replication + "\n" + `//ptah:schema:async_replication:item replication="mirror" source="a" target="/local/ra"`,
			wantErr:     `invalid target "/local/ra": takes a path relative to the database root, .*`},
		{name: "a transfer with no lambda",
			annotations: `//ptah:schema:transfer name="ingest" source="tp" target="t"`,
			wantErr:     `missing required annotation attribute "using" on //ptah:schema:transfer at Replicas`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource(noOwners, "replicas.go", replicationSource(test.annotations))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.DeepEquals, schemamodel.Database{})
		})
	}
}

// TestParseSource_AsyncReplication_RefusesAnInvalidValueAsSuch pins the
// sentinels a replication declaration is refused with.
func TestParseSource_AsyncReplication_RefusesAnInvalidValueAsSuch(t *testing.T) {
	c := qt.New(t)
	_, err := goschema.ParseSource(noOwners, "replicas.go", replicationSource(
		`//ptah:schema:transfer name="ingest" source="tp" target="t" using="($m) -> { return []; }" batch_size_bytes="0"`))
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
	_, err = goschema.ParseSource(noOwners, "replicas.go", replicationSource(
		`//ptah:schema:async_replication name="mirror" connection_string="grpc://p:2136/?database=/prod"`))
	c.Assert(err, qt.ErrorIs, ptaherr.ErrMissingRequiredAttribute)
}
