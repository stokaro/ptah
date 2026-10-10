package ydbdiff_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/engine"
)

var (
	mirror = ydbreplication.ReplicationSpec{
		Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod"},
		Items:      []ydbreplication.Item{{Source: "a", Target: "ra"}},
	}
	movedMirror = ydbreplication.ReplicationSpec{
		Connection: ydbreplication.Connection{ConnectionString: "grpc://standby:2136/?database=/prod"},
		Items:      []ydbreplication.Item{{Source: "a", Target: "ra"}},
	}
	ingest      = ydbreplication.TransferSpec{Source: "tp", Target: "t", Lambda: "($m) -> { return []; }"}
	reingest    = ydbreplication.TransferSpec{Source: "tp", Target: "t", Lambda: "($m) -> { return [<| id: 1 |>]; }"}
	rebatched   = ydbreplication.TransferSpec{Source: "tp", Target: "t", Lambda: "($m) -> { return []; }", BatchSizeBytes: 1024}
	reconnected = ydbreplication.TransferSpec{Source: "tp", Target: "t", Lambda: "($m) -> { return []; }",
		Connection: ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod"}}
)

// TestReplicationChange_RoundTrip keeps both operands, the state included,
// through the registered change codecs.
func TestReplicationChange_RoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		change schemaext.Payload
	}{
		{name: "a created replication", change: &ydbdiff.AsyncReplication{After: &ydbreplication.DesiredReplication{Spec: mirror, StructName: "M"}}},
		{name: "a dropped replication", change: &ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: mirror,
			State: ydbreplication.StateDone}}},
		{name: "a moved replication", change: &ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: mirror,
			State: ydbreplication.StatePaused}, After: &ydbreplication.DesiredReplication{Spec: movedMirror}}},
		{name: "a changed transfer", change: &ydbdiff.Transfer{Before: &ydbreplication.ObservedTransfer{Spec: ingest},
			After: &ydbreplication.DesiredTransfer{Spec: reingest}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			registry := must.Must(engine.New(engine.Provider{ID: "ptah.run/ydb", Codecs: ydbdiff.Codecs()})).Codecs()
			document, err := registry.Marshal(t.Context(), schemaext.Change, []schemaext.Payload{test.change})
			c.Assert(err, qt.IsNil)
			decoded, err := registry.Unmarshal(t.Context(), document)
			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{test.change})
		})
	}
}

// TestReplicationChange_FailurePath refuses a change without explicit
// operands, an operand its model codec refuses, a field no change takes, and
// two operands that describe one object as YDB keeps it.
func TestReplicationChange_FailurePath(t *testing.T) {
	const spec = `{"connection":{"connection_string":"grpc://h:2136/?database=/prod"},"items":[{"source":"a","target":"ra"}]}`
	const same = `{"connection":{"connection_string":"grpc://h:2136/?database=/prod"},"items":[{"source":"a","target":"ra"}],` +
		`"consistency_level":"row"}`
	tests := []struct {
		name  string
		codec schemaext.Codec
		input string
	}{
		{name: "null", codec: ydbdiff.AsyncReplicationCodec(), input: `null`},
		{name: "no operands", codec: ydbdiff.AsyncReplicationCodec(), input: `{"before":null,"after":null}`},
		{name: "an omitted operand", codec: ydbdiff.AsyncReplicationCodec(), input: `{"after":{"spec":` + spec + `}}`},
		{name: "a state on a declaration", codec: ydbdiff.AsyncReplicationCodec(),
			input: `{"before":null,"after":{"spec":` + spec + `,"state":"running"}}`},
		{name: "an empty state", codec: ydbdiff.AsyncReplicationCodec(), input: `{"before":{"spec":` + spec + `,"state":""},"after":null}`},
		{name: "an extra field", codec: ydbdiff.AsyncReplicationCodec(), input: `{"before":null,"after":{"spec":` + spec + `},"cascade":true}`},
		{name: "a default spelled out", codec: ydbdiff.AsyncReplicationCodec(),
			input: `{"before":{"spec":` + spec + `},"after":{"spec":` + same + `}}`},
		{name: "a transfer operand in a replication change", codec: ydbdiff.AsyncReplicationCodec(),
			input: `{"before":null,"after":{"spec":{"source":"t","target":"l","lambda":"($m) -> { return []; }"}}}`},
		{name: "the same transfer", codec: ydbdiff.TransferCodec(),
			input: `{"before":{"spec":{"source":"t","target":"l","lambda":"($m) -> { return []; }"}},` +
				`"after":{"spec":{"source":"t","target":"l","lambda":" ($m) -> { return []; } "}}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			value, err := test.codec.Decode(json.RawMessage(test.input))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

// TestReplicationChange_Effect reports what each change does: a drop takes
// the replica tables along unless the replication was failed over, a dropped
// transfer loses the consumer YDB made for it, and a change in place needs
// review for what it changed.
func TestReplicationChange_Effect(t *testing.T) {
	tests := []struct {
		name   string
		change interface{ Effect() schemaext.Effect }
		want   schemaext.Effect
	}{
		{name: "a created replication", change: &ydbdiff.AsyncReplication{After: &ydbreplication.DesiredReplication{Spec: mirror}},
			want: schemaext.Effect{Impact: schemaext.Additive, Reason: "CREATE ASYNC REPLICATION adds a replication and its replica tables"}},
		{name: "a running replication dropped", change: &ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: mirror,
			State: ydbreplication.StateRunning}},
			want: schemaext.Effect{Impact: schemaext.Destructive, Reason: ydbdiff.DropReplicationReason}},
		{name: "a replication of an unread state dropped", change: &ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: mirror}},
			want: schemaext.Effect{Impact: schemaext.Destructive, Reason: ydbdiff.DropReplicationReason}},
		{name: "a failed-over replication dropped", change: &ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: mirror,
			State: ydbreplication.StateDone}},
			want: schemaext.Effect{Impact: schemaext.Behavioral, Reason: ydbdiff.KeepReplicaTablesReason}},
		{name: "a moved replication", change: &ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: mirror},
			After: &ydbreplication.DesiredReplication{Spec: movedMirror}},
			want: schemaext.Effect{Impact: schemaext.Behavioral, Reason: ydbdiff.AlterReplicationReason}},
		{name: "a created transfer", change: &ydbdiff.Transfer{After: &ydbreplication.DesiredTransfer{Spec: ingest}},
			want: schemaext.Effect{Impact: schemaext.Additive, Reason: "CREATE TRANSFER adds a transfer"}},
		{name: "a dropped transfer", change: &ydbdiff.Transfer{Before: &ydbreplication.ObservedTransfer{Spec: ingest}},
			want: schemaext.Effect{Impact: schemaext.Destructive, Reason: ydbdiff.DropTransferReason}},
		{name: "a new lambda", change: &ydbdiff.Transfer{Before: &ydbreplication.ObservedTransfer{Spec: ingest},
			After: &ydbreplication.DesiredTransfer{Spec: reingest}},
			want: schemaext.Effect{Impact: schemaext.Behavioral, Reason: ydbdiff.AlterTransferReason}},
		{name: "a new batch size", change: &ydbdiff.Transfer{Before: &ydbreplication.ObservedTransfer{Spec: ingest},
			After: &ydbreplication.DesiredTransfer{Spec: rebatched}},
			want: schemaext.Effect{Impact: schemaext.Behavioral, Reason: ydbdiff.AlterTransferReason}},
		{name: "a new connection", change: &ydbdiff.Transfer{Before: &ydbreplication.ObservedTransfer{Spec: ingest},
			After: &ydbreplication.DesiredTransfer{Spec: reconnected}},
			want: schemaext.Effect{Impact: schemaext.Behavioral, Reason: ydbdiff.ReconnectTransferReason}},
		{name: "an invalid change", change: &ydbdiff.Transfer{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.change.Effect(), qt.DeepEquals, test.want)
		})
	}
}
