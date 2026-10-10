package ydbast_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreplication"
)

func replicationRegistry(c *qt.C) schemaext.Registry {
	c.Helper()
	registry, err := schemaext.NewRegistry(
		schemaext.OwnedCodec{Owner: "ptah.run/ydb", Codec: ydbast.AsyncReplicationCodec()},
		schemaext.OwnedCodec{Owner: "ptah.run/ydb", Codec: ydbast.TransferCodec()})
	c.Assert(err, qt.IsNil)
	return registry
}

var replicationSpec = ydbreplication.ReplicationSpec{
	Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod"},
	Items:      []ydbreplication.Item{{Source: "a", Target: "ra"}},
}

// TestReplicationCodecs_HappyPath round-trips each statement on a replication
// and a transfer with its separate path parts and the state its before
// operand carries.
func TestReplicationCodecs_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		value schemaext.Payload
	}{
		{name: "a replication drop", value: &ydbast.AsyncReplication{Schema: "jobs.daily", Name: "mirror.v1",
			Change: ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: replicationSpec, State: ydbreplication.StateDone}}}},
		{name: "a transfer creation", value: &ydbast.Transfer{Name: "ingest", Change: ydbdiff.Transfer{After: &ydbreplication.DesiredTransfer{
			Spec: ydbreplication.TransferSpec{Source: "tp", Target: "t", Lambda: "($m) -> { return []; }"}}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			data, err := replicationRegistry(c).Marshal(c.Context(), schemaext.Operation, []schemaext.Payload{test.value})
			c.Assert(err, qt.IsNil)
			decoded, err := replicationRegistry(c).Unmarshal(c.Context(), data)
			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{test.value})
		})
	}
}

// TestReplicationCodecs_FailurePath refuses a statement wire that is
// incomplete, names no object, or carries operands of the other kind.
func TestReplicationCodecs_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		codec schemaext.Codec
		data  string
	}{
		{name: "no change", codec: ydbast.AsyncReplicationCodec(), data: `{"schema":"","name":"mirror"}`},
		{name: "no name", codec: ydbast.AsyncReplicationCodec(),
			data: `{"schema":"","name":"","change":{"before":{"spec":{"connection":{}}},"after":null}}`},
		{name: "a slash in the name", codec: ydbast.TransferCodec(),
			data: `{"schema":"","name":"a/b","change":{"before":{"spec":{"source":"t","target":"l","lambda":"x"}},"after":null}}`},
		{name: "transfer operands", codec: ydbast.AsyncReplicationCodec(),
			data: `{"schema":"","name":"mirror","change":{"before":{"spec":{"source":"t","target":"l","lambda":"x"}},"after":null}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			value, err := test.codec.Decode(json.RawMessage(test.data))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

// TestReplicationOperation_Identity names the object by its directory and
// leaf, and by the canonical reference the statements take.
func TestReplicationOperation_Identity(t *testing.T) {
	c := qt.New(t)
	operation := &ydbast.AsyncReplication{Schema: "app", Name: "mirror.v1"}
	c.Assert(operation.Subject(), qt.Equals, ydbreplication.ReplicationRef("app", "mirror.v1"))
	c.Assert(operation.Reference(), qt.Equals, `app."mirror.v1"`)
	transfer := &ydbast.Transfer{Name: "ingest"}
	c.Assert(transfer.Subject(), qt.Equals, ydbreplication.TransferRef("", "ingest"))
	c.Assert(transfer.Reference(), qt.Equals, "ingest")
}
