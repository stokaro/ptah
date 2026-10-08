package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

// TestRender_Replication_RoundTrip writes a YDB async replication with its
// items and a transfer as annotations that parse back to the same
// declarations, so `ptah introspect` of a YDB database keeps them. The lambda
// carries quotes and a newline, which an attribute value has to keep.
func TestRender_Replication_RoundTrip(t *testing.T) {
	c := qt.New(t)
	replication := schemamodel.AsyncReplication{Name: "mirror", Schema: "dr", Spec: ast.AsyncReplicationSpec{
		Connection: ast.ReplicationConnectionSpec{ConnectionString: "grpcs://primary:2135/?database=/prod",
			User: "replicator", PasswordSecretPath: "secrets/password"},
		Items: []ast.AsyncReplicationItem{
			{Source: "accounts", Target: "replica/accounts"},
			{Source: "/prod/ledger", Target: "replica/ledger"},
		},
		ConsistencyLevel: "global",
		CommitInterval:   "PT1.5S",
	}}
	transfer := schemamodel.Transfer{Name: "ingest", Spec: ast.TransferSpec{
		Connection: ast.ReplicationConnectionSpec{ConnectionString: "grpc://primary:2136/?database=/prod",
			TokenSecretName: "token"},
		Source: "events", Target: "log",
		Lambda:   "($m) -> {\n  return [<| a: CAST($m._data AS Utf8), b: \"it's\" |>];\n}",
		Consumer: "ingest", BatchSizeBytes: 1048576, FlushInterval: "PT10S",
	}}
	db := &schemamodel.Database{
		AsyncReplications: []schemamodel.AsyncReplication{replication},
		Transfers: []schemamodel.Transfer{transfer, {Name: "plain", Spec: ast.TransferSpec{Source: "tp", Target: "t",
			Lambda: "($m) -> { return []; }"}}},
	}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{PackageName: "models", SingleFile: true, Dialect: "ydb"})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	parsed, err := goschema.ParseSource(files[0].Name, files[0].Data)

	c.Assert(err, qt.IsNil)
	c.Assert(parsed.AsyncReplications, qt.HasLen, 1)
	c.Assert(parsed.AsyncReplications[0].Spec, qt.DeepEquals, replication.Spec)
	c.Assert([]string{parsed.AsyncReplications[0].Name, parsed.AsyncReplications[0].Schema}, qt.DeepEquals,
		[]string{"mirror", "dr"})
	c.Assert(parsed.Transfers, qt.HasLen, 2)
	c.Assert(parsed.Transfers[0].Spec, qt.DeepEquals, transfer.Spec)
	c.Assert(parsed.Transfers[1].Name, qt.Equals, "plain")
}
