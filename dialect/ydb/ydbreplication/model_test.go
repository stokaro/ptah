package ydbreplication_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/engine"
)

// fullReplication names every setting a replication of a user with a password
// secret takes, and two items.
func fullReplication() ydbreplication.ReplicationSpec {
	return ydbreplication.ReplicationSpec{
		Connection: ydbreplication.Connection{ConnectionString: "grpcs://primary.example.com:2135/?database=/prod",
			User: "replicator", PasswordSecretPath: "secrets/replicator"},
		Items:            []ydbreplication.Item{{Source: "accounts", Target: "replica/accounts"}, {Source: "/prod/ledger", Target: "replica/ledger"}},
		ConsistencyLevel: ydbreplication.ConsistencyGlobal,
		CommitInterval:   "PT30S",
	}
}

// fullTransfer names every setting a transfer of another database's topic
// takes.
func fullTransfer() ydbreplication.TransferSpec {
	return ydbreplication.TransferSpec{
		Connection: ydbreplication.Connection{ConnectionString: "grpc://primary.example.com:2136/?database=/prod",
			TokenSecretName: "token"},
		Source: "events", Target: "app/log", Lambda: "($msg) -> { return [<| id: $msg._offset |>]; }",
		Consumer: "ingest", BatchSizeBytes: 1048576, FlushInterval: "PT10S",
	}
}

// TestModelTransport_KeepsEverySetting round-trips each model through the
// registered codecs: every setting, the holder and the state survive.
func TestModelTransport_KeepsEverySetting(t *testing.T) {
	tests := []struct {
		name           string
		representation schemaext.Representation
		object         schemaext.Object
	}{
		{name: "a declared replication", representation: schemaext.Desired,
			object: ydbreplication.DesiredReplicationObject("app", "mirror.v1", "Mirror", fullReplication())},
		{name: "an observed replication", representation: schemaext.Observed,
			object: ydbreplication.ObservedReplicationObject("app", "mirror", fullReplication(), ydbreplication.StatePaused)},
		{name: "a declared transfer", representation: schemaext.Desired,
			object: ydbreplication.DesiredTransferObject("", "ingest", "Ingest", fullTransfer())},
		{name: "an observed transfer", representation: schemaext.Observed,
			object: ydbreplication.ObservedTransferObject("etl", "ingest", fullTransfer(), ydbreplication.StateRunning)},
		{name: "a transfer of a topic of its own database", representation: schemaext.Desired,
			object: ydbreplication.DesiredTransferObject("", "local", "", ydbreplication.TransferSpec{Source: "events", Target: "log",
				Lambda: "($m) -> { return []; }"})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(engine.New(engine.Provider{ID: "ptah.run/ydb", Codecs: ydbreplication.Codecs()}))
			objects := must.Must(schemaext.NewObjects(test.object))

			data, err := runtime.Codecs().EncodeObjects(t.Context(), test.representation, objects)
			c.Assert(err, qt.IsNil)
			decoded, err := runtime.Codecs().DecodeObjects(t.Context(), test.representation, data)

			c.Assert(err, qt.IsNil)
			values, err := decoded.All()
			c.Assert(err, qt.IsNil)
			c.Assert(values, qt.DeepEquals, []schemaext.Object{test.object})
		})
	}
}

// TestModelCodecs_RefuseALossyWire refuses a document that would decode to an
// object other than the one written, or to one written two ways: an unknown
// key at any depth, a null, an omitted value written out, a missing required
// key, and a value the representation cannot hold.
func TestModelCodecs_RefuseALossyWire(t *testing.T) {
	const connection = `"connection":{"connection_string":"grpc://h:2136/?database=/prod"}`
	const items = `"items":[{"source":"a","target":"ra"}]`
	tests := []struct {
		name  string
		codec schemaext.Codec
		input string
	}{
		{name: "an unknown key", codec: ydbreplication.ReplicationCodecs()[0], input: `{"spec":{` + connection + `,` + items + `},"state":"running"}`},
		{name: "an unknown spec key", codec: ydbreplication.ReplicationCodecs()[0], input: `{"spec":{` + connection + `,` + items + `,"mode":"x"}}`},
		{name: "an unknown connection key", codec: ydbreplication.ReplicationCodecs()[1],
			input: `{"spec":{"connection":{"connection_string":"grpc://h:2136/?database=/prod","password":"p"},` + items + `}}`},
		{name: "an unknown item key", codec: ydbreplication.ReplicationCodecs()[1],
			input: `{"spec":{` + connection + `,"items":[{"source":"a","target":"ra","mode":"x"}]}}`},
		{name: "a key in another case", codec: ydbreplication.ReplicationCodecs()[0], input: `{"Spec":{` + connection + `,` + items + `}}`},
		{name: "a null", codec: ydbreplication.ReplicationCodecs()[0], input: `{"spec":{` + connection + `,` + items + `,"commit_interval":null}}`},
		{name: "an empty holder", codec: ydbreplication.ReplicationCodecs()[0], input: `{"spec":{` + connection + `,` + items + `},"struct_name":""}`},
		{name: "an empty item list", codec: ydbreplication.ReplicationCodecs()[1], input: `{"spec":{` + connection + `,"items":[]}}`},
		{name: "an empty state", codec: ydbreplication.ReplicationCodecs()[1], input: `{"spec":{` + connection + `},"state":""}`},
		{name: "an empty credential", codec: ydbreplication.ReplicationCodecs()[1],
			input: `{"spec":{"connection":{"connection_string":"grpc://h:2136/?database=/prod","user":""}}}`},
		{name: "no spec", codec: ydbreplication.ReplicationCodecs()[0], input: `{"struct_name":"Mirror"}`},
		{name: "no connection", codec: ydbreplication.ReplicationCodecs()[1], input: `{"spec":{` + items + `}}`},
		{name: "a declared replication without items", codec: ydbreplication.ReplicationCodecs()[0], input: `{"spec":{` + connection + `}}`},
		{name: "an unknown state", codec: ydbreplication.ReplicationCodecs()[1], input: `{"spec":{` + connection + `},"state":"stopped"}`},
		{name: "a commit interval at the row level", codec: ydbreplication.ReplicationCodecs()[1],
			input: `{"spec":{` + connection + `,"commit_interval":"PT1S"}}`},
		{name: "an empty transfer connection", codec: ydbreplication.TransferCodecs()[0],
			input: `{"spec":{"connection":{},"source":"t","target":"l","lambda":"($m) -> { return []; }"}}`},
		{name: "an item key in another case", codec: ydbreplication.ReplicationCodecs()[1],
			input: `{"spec":{` + connection + `,"items":[{"Source":"a","target":"ra"}]}}`},
		{name: "an empty transfer credential", codec: ydbreplication.TransferCodecs()[1],
			input: `{"spec":{"connection":{"connection_string":"grpc://h:2136/?database=/prod","user":""},"source":"t","target":"l",` +
				`"lambda":"($m) -> { return []; }"}}`},
		{name: "an unknown transfer connection key", codec: ydbreplication.TransferCodecs()[1],
			input: `{"spec":{"connection":{"endpoint":"h:2136"},"source":"t","target":"l","lambda":"($m) -> { return []; }"}}`},
		{name: "a zero batch size", codec: ydbreplication.TransferCodecs()[1],
			input: `{"spec":{"source":"t","target":"l","lambda":"($m) -> { return []; }","batch_size_bytes":0}}`},
		{name: "a transfer without a lambda", codec: ydbreplication.TransferCodecs()[1], input: `{"spec":{"source":"t","target":"l"}}`},
		{name: "an empty observed lambda", codec: ydbreplication.TransferCodecs()[1], input: `{"spec":{"source":"t","target":"l","lambda":""}}`},
		{name: "an unknown observed consistency level", codec: ydbreplication.ReplicationCodecs()[1],
			input: `{"spec":{` + connection + `,"consistency_level":"eventual"}}`},
		{name: "a declared lambda ending the statement", codec: ydbreplication.TransferCodecs()[0],
			input: `{"spec":{"source":"t","target":"l","lambda":"($m) -> { return []; };"}}`},
		{name: "a holder on an observation", codec: ydbreplication.TransferCodecs()[1],
			input: `{"spec":{"source":"t","target":"l","lambda":"($m) -> { return []; }"},"struct_name":"T"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			value, err := test.codec.Decode(json.RawMessage(test.input))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorAs, new(*schemaext.InvalidModelError))
			c.Assert(value, qt.IsNil)
		})
	}
}

// TestModelValidate_FailurePath refuses a value no statement or read carries
// faithfully: text that is not valid UTF-8 or holds a NUL, a declaration the
// parse would refuse, and an observation the reader cannot write.
func TestModelValidate_FailurePath(t *testing.T) {
	withText := func(text string) ydbreplication.ReplicationSpec {
		spec := fullReplication()
		spec.Items[0].Source = text
		return spec
	}
	noConnection := fullReplication()
	noConnection.Connection = ydbreplication.Connection{}
	blankItem := fullReplication()
	blankItem.Items[0].Target = ""
	tests := []struct {
		name  string
		value interface{ Validate() error }
	}{
		{name: "an invalid UTF-8 holder", value: &ydbreplication.DesiredReplication{Spec: fullReplication(), StructName: "\xff"}},
		{name: "a NUL in an item", value: &ydbreplication.ObservedReplication{Spec: withText("a\x00b")}},
		{name: "a declaration without a connection", value: &ydbreplication.DesiredReplication{Spec: noConnection}},
		{name: "an observed item without a target", value: &ydbreplication.ObservedReplication{Spec: blankItem}},
		{name: "a declared transfer with a credential and no connection string",
			value: &ydbreplication.DesiredTransfer{Spec: ydbreplication.TransferSpec{Source: "t", Target: "l",
				Lambda: "($m) -> { return []; }", Connection: ydbreplication.Connection{TokenSecretName: "token"}}}},
		{name: "an observed transfer with a fractional flush interval",
			value: &ydbreplication.ObservedTransfer{Spec: ydbreplication.TransferSpec{Source: "t", Target: "l",
				Lambda: "($m) -> { return []; }", FlushInterval: "PT1.5S"}}},
		{name: "a nil declaration", value: (*ydbreplication.DesiredTransfer)(nil)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.value.Validate(), qt.ErrorIs, schemaext.ErrInvalidValue)
		})
	}
}

// TestModelValidate_ObservationsTheReaderWrites accepts what a read reports
// and no declaration may say: a replication that resolved no table, a secret
// path outside the database, and a lambda stored in a form no declaration
// writes.
func TestModelValidate_ObservationsTheReaderWrites(t *testing.T) {
	noItems := fullReplication()
	noItems.Items = nil
	outside := fullReplication()
	outside.Connection.PasswordSecretPath = "/other/secrets/replicator"
	tests := []struct {
		name  string
		value interface{ Validate() error }
	}{
		{name: "a replication with no item", value: &ydbreplication.ObservedReplication{Spec: noItems, State: ydbreplication.StateError}},
		{name: "a secret path outside the database", value: &ydbreplication.ObservedReplication{Spec: outside}},
		{name: "a stored named lambda", value: &ydbreplication.ObservedTransfer{Spec: ydbreplication.TransferSpec{Source: "t",
			Target: "l", Lambda: "$f = ($m) -> { return []; };\n$__ydb_transfer_lambda = $f;\n"}, State: ydbreplication.StateDone}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.value.Validate(), qt.IsNil)
		})
	}
}

// TestModelClone_IsIndependent changes a clone's items and leaves the
// original as it was.
func TestModelClone_IsIndependent(t *testing.T) {
	c := qt.New(t)
	original := &ydbreplication.ObservedReplication{Spec: fullReplication(), State: ydbreplication.StateRunning}

	clone := original.Clone().(*ydbreplication.ObservedReplication)
	clone.Spec.Items[0].Target = "elsewhere"

	c.Assert(original, qt.DeepEquals, &ydbreplication.ObservedReplication{Spec: fullReplication(), State: ydbreplication.StateRunning})
}

// TestModelEqual_ComparesEveryField tells two values apart by any field,
// the holder and the state included, and resolves no default.
func TestModelEqual_ComparesEveryField(t *testing.T) {
	reordered := fullReplication()
	reordered.Items[0], reordered.Items[1] = reordered.Items[1], reordered.Items[0]
	defaulted := fullTransfer()
	defaulted.BatchSizeBytes = ydbreplication.DefaultBatchSizeBytes
	tests := []struct {
		name        string
		left, right schemaext.Value
		want        bool
	}{
		{name: "the same declaration", left: &ydbreplication.DesiredReplication{Spec: fullReplication(), StructName: "M"},
			right: &ydbreplication.DesiredReplication{Spec: fullReplication(), StructName: "M"}, want: true},
		{name: "another holder", left: &ydbreplication.DesiredReplication{Spec: fullReplication(), StructName: "M"},
			right: &ydbreplication.DesiredReplication{Spec: fullReplication()}, want: false},
		{name: "items in another order", left: &ydbreplication.DesiredReplication{Spec: fullReplication()},
			right: &ydbreplication.DesiredReplication{Spec: reordered}, want: false},
		{name: "another state", left: &ydbreplication.ObservedTransfer{Spec: fullTransfer(), State: ydbreplication.StatePaused},
			right: &ydbreplication.ObservedTransfer{Spec: fullTransfer()}, want: false},
		{name: "a default written out", left: &ydbreplication.DesiredTransfer{Spec: fullTransfer()},
			right: &ydbreplication.DesiredTransfer{Spec: defaulted}, want: false},
		{name: "another kind", left: &ydbreplication.DesiredTransfer{Spec: fullTransfer()},
			right: &ydbreplication.ObservedTransfer{Spec: fullTransfer()}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.left.Equal(test.right), qt.Equals, test.want)
		})
	}
}

// TestConversion_KeepsTheObject turns an observation into a declaration
// without its state, and a declaration into an observation in the state
// given.
func TestConversion_KeepsTheObject(t *testing.T) {
	c := qt.New(t)
	c.Assert((&ydbreplication.ObservedReplication{Spec: fullReplication(), State: ydbreplication.StateRunning}).Desired(),
		qt.DeepEquals, &ydbreplication.DesiredReplication{Spec: fullReplication()})
	c.Assert((&ydbreplication.DesiredTransfer{Spec: fullTransfer(), StructName: "T"}).Observed(ydbreplication.StatePaused),
		qt.DeepEquals, &ydbreplication.ObservedTransfer{Spec: fullTransfer(), State: ydbreplication.StatePaused})
}

// TestValidateIdentity_FailurePath refuses an identity that is not a
// directory and a leaf of one of the two kinds.
func TestValidateIdentity_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		ref  objectidentity.ID
	}{
		{name: "another kind", ref: objectidentity.ID{Kind: "ptah.run/ydb/topic", Name: ydbreplication.ReplicationRef("", "x").Name}},
		{name: "a slash in the name", ref: ydbreplication.TransferRef("", "a/b")},
		{name: "an absolute directory", ref: ydbreplication.ReplicationRef("/app", "x")},
		{name: "a parent directory", ref: ydbreplication.ReplicationRef("../app", "x")},
		{name: "no name", ref: ydbreplication.TransferRef("app", " ")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbreplication.ValidateIdentity(test.ref), qt.ErrorIs, schemaext.ErrInvalidValue)
		})
	}
}

// TestValidateIdentity_HappyPath accepts a directory and a leaf with a dot,
// which is part of the leaf.
func TestValidateIdentity_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(ydbreplication.ValidateIdentity(ydbreplication.ReplicationRef("app/etl", "mirror.v1")), qt.IsNil)
	c.Assert(ydbreplication.ValidateIdentity(ydbreplication.TransferRef("", "ingest")), qt.IsNil)
	c.Assert(ydbreplication.Reference("app/etl", "mirror.v1"), qt.Equals, `app/etl."mirror.v1"`)
	c.Assert(ydbreplication.Display("app/etl", "mirror.v1"), qt.Equals, "app/etl/mirror.v1")
}

// TestRollbackTarget keeps a credential the forward change added, which YDB
// cannot take away, and restores everything else.
func TestRollbackTarget(t *testing.T) {
	c := qt.New(t)
	before := fullTransfer()
	before.Connection.TokenSecretName = ""
	after := fullTransfer()
	after.Connection.ConnectionString = "grpc://standby.example.com:2136/?database=/prod"

	got := ydbreplication.TransferRollbackTarget(before, after)

	want := before
	want.Connection.TokenSecretName = "token"
	c.Assert(got, qt.Equals, want)
	c.Assert(ydbreplication.TransferRollbackTarget(fullTransfer(), after), qt.Equals, fullTransfer())
	replication := ydbreplication.ReplicationRollbackTarget(fullReplication(), fullReplication())
	c.Assert(replication, qt.DeepEquals, fullReplication())
}
