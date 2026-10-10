package ydbconvert_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbconvert"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/engine"
)

// TestReplicationConversion_KeepsEverySetting converts a declaration into the
// observation a read would report once it is applied, without the holder and
// with no state, and that observation back into a declaration that keeps the
// object.
func TestReplicationConversion_KeepsEverySetting(t *testing.T) {
	mirror := ydbreplication.ReplicationSpec{
		Connection:       ydbreplication.Connection{ConnectionString: "grpc://primary:2136/?database=/prod", TokenSecretName: "token"},
		Items:            []ydbreplication.Item{{Source: "a", Target: "ra"}},
		ConsistencyLevel: ydbreplication.ConsistencyGlobal, CommitInterval: "PT5S",
	}
	ingest := ydbreplication.TransferSpec{Source: "tp", Target: "t", Lambda: "($m) -> { return []; }", Consumer: "c", FlushInterval: "PT5S"}
	tests := []struct {
		name               string
		kind               schemaext.Kind
		service            schemaext.ConversionService
		desired            schemaext.Value
		observed, restored schemaext.Value
	}{
		{name: "a replication", kind: ydbreplication.ReplicationKind, service: ydbconvert.AsyncReplicationService{},
			desired:  &ydbreplication.DesiredReplication{Spec: mirror, StructName: "Mirror"},
			observed: &ydbreplication.ObservedReplication{Spec: mirror}, restored: &ydbreplication.DesiredReplication{Spec: mirror}},
		{name: "a transfer", kind: ydbreplication.TransferKind, service: ydbconvert.TransferService{},
			desired:  &ydbreplication.DesiredTransfer{Spec: ingest, StructName: "Ingest"},
			observed: &ydbreplication.ObservedTransfer{Spec: ingest}, restored: &ydbreplication.DesiredTransfer{Spec: ingest}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime, err := engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}}, Codecs: ydbreplication.Codecs(),
				Conversions: []engine.Conversion{{Target: "ydb", Kinds: []schemaext.Kind{test.kind}, Service: test.service}}})
			c.Assert(err, qt.IsNil)

			observed, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "ydb", From: schemaext.Desired,
				To: schemaext.Observed, Values: []schemaext.Value{test.desired}})
			c.Assert(err, qt.IsNil)
			restored, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "ydb", From: schemaext.Observed,
				To: schemaext.Desired, Values: observed})
			c.Assert(err, qt.IsNil)

			c.Assert(observed, qt.DeepEquals, []schemaext.Value{test.observed})
			c.Assert(restored, qt.DeepEquals, []schemaext.Value{test.restored})
		})
	}
}
