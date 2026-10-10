package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

// TestRender_Replication_RoundTrip writes a YDB async replication with its
// items and a transfer as annotations that parse back to the same
// declarations, so `ptah introspect` of a YDB database keeps them. The lambda
// carries quotes and a newline, which an attribute value has to keep.
func TestRender_Replication_RoundTrip(t *testing.T) {
	c := qt.New(t)
	replication := ydbreplication.ReplicationSpec{
		Connection: ydbreplication.Connection{ConnectionString: "grpcs://primary:2135/?database=/prod",
			User: "replicator", PasswordSecretPath: "secrets/password"},
		Items: []ydbreplication.Item{
			{Source: "accounts", Target: "replica/accounts"},
			{Source: "/prod/ledger", Target: "replica/ledger"},
		},
		ConsistencyLevel: "global",
		CommitInterval:   "PT1.5S",
	}
	transfer := ydbreplication.TransferSpec{
		Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod",
			TokenSecretName: "token"},
		Source: "events", Target: "log",
		Lambda:   "($m) -> {\n  return [<| a: CAST($m._data AS Utf8), b: \"it's\" |>];\n}",
		Consumer: "ingest", BatchSizeBytes: 1048576, FlushInterval: "PT10S",
	}
	plain := ydbreplication.TransferSpec{Source: "tp", Target: "t", Lambda: "($m) -> { return []; }"}
	db := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		ydbreplication.DesiredReplicationObject("dr", "mirror", "", replication),
		ydbreplication.DesiredTransferObject("", "ingest", "", transfer),
		ydbreplication.DesiredTransferObject("", "plain", "", plain),
	))}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{PackageName: "models", SingleFile: true, Dialect: "ydb"})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	parsed, err := goschema.ParseSource(builtintest.Annotations(), files[0].Name, files[0].Data)

	c.Assert(err, qt.IsNil)
	objects, err := parsed.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.HasLen, 3)
	c.Assert(objects[0].Ref, qt.Equals, ydbreplication.ReplicationRef("dr", "mirror"))
	c.Assert(objects[0].Value.(*ydbreplication.DesiredReplication).Spec, qt.DeepEquals, replication)
	c.Assert(objects[1].Ref, qt.Equals, ydbreplication.TransferRef("", "ingest"))
	c.Assert(objects[1].Value.(*ydbreplication.DesiredTransfer).Spec, qt.DeepEquals, transfer)
	c.Assert(objects[2].Value.(*ydbreplication.DesiredTransfer).Spec, qt.DeepEquals, plain)
}
