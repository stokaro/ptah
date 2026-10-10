package ydbrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbrender"
	"ptah.run/dialect/ydb/ydbreplication"
)

var renderedIngest = &ydbast.Transfer{Name: "ingest", Change: ydbdiff.Transfer{After: &ydbreplication.DesiredTransfer{
	Spec: ydbreplication.TransferSpec{Source: "tp", Target: "t", Lambda: "($m) -> { return []; }"}}}}

// TestReplicationHandlers_RefuseAnotherTarget refuses a statement handed to
// either handler for a target other than YDB, by the kind's key, so an
// embedder that registers the handler elsewhere writes nothing.
func TestReplicationHandlers_RefuseAnotherTarget(t *testing.T) {
	tests := []struct {
		name    string
		handler renderer.ExtensionHandler
		payload ast.ExtensionPayload
		wantErr string
	}{
		{name: "a replication", handler: ydbrender.AsyncReplicationHandler(), payload: &ydbast.AsyncReplication{Name: "mirror",
			Change: ydbdiff.AsyncReplication{Before: &ydbreplication.ObservedReplication{Spec: ydbreplication.ReplicationSpec{}}}},
			wantErr: `async replication mirror, which requires target capability async_replication, unavailable on this postgres target`},
		{name: "a transfer", handler: ydbrender.TransferHandler(), payload: renderedIngest,
			wantErr: `transfer ingest, which requires target capability transfers, unavailable on this postgres target`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.Postgres18().With(capability.AsyncReplication, true).With(capability.Transfers, true)
			err := test.handler.Validate(renderer.ExtensionContext{Target: "postgres", Capabilities: caps}, test.payload)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		})
	}
}

// TestReplicationHandlers_AcceptYDB is the control: the same transfer
// validates on a YDB line with the key.
func TestReplicationHandlers_AcceptYDB(t *testing.T) {
	c := qt.New(t)
	err := ydbrender.TransferHandler().Validate(renderer.ExtensionContext{Target: "ydb", Capabilities: capability.YDB262()}, renderedIngest)
	c.Assert(err, qt.IsNil)
}
