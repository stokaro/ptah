package providerhost

// White-box testing required: the transport validates batch provenance before
// this private adapter binds the returned body to the captured operation model.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
)

func nativeRenderFixture() (*target, *protocol.Change, *protocol.Unit) {
	owner := &protocol.Reference{Engine: "fixture", Kind: "table", Name: "records"}
	value := &protocol.Envelope{ProviderId: "example.org/fixture", Kind: "example.org/fixture/create", SchemaVersion: 1, Encoding: "json", Owner: owner, Payload: []byte(`{"create":true}`)}
	normalized := &protocol.NormalizedValue{Value: value, NormalizedOwner: proto.CloneOf(owner), CanonicalFormat: "fixture", CanonicalProjection: []byte(`{"create":true}`), OperationSemantics: &protocol.OperationSemantics{Impact: protocol.ChangeSemantics_ADDITIVE, Reason: "create", Access: protocol.ChangeSemantics_UNCHANGED, AccessReason: "no access change"}}
	key := modelKey{kind: schemaext.Kind(value.Kind), representation: schemaext.Operation}
	contract := &modelContract{key: key, signature: "fixture", engines: map[string]*protocol.ValueModel{"fixture": {Kind: value.Kind}}}
	selected := &target{engine: &protocol.Engine{Id: "fixture"}, host: &Host{models: map[modelKey]*modelContract{key: contract}}}
	change := &protocol.Change{Id: "0", Object: owner, Operation: &protocol.Change_Native{Native: &protocol.NativeOperation{Role: protocol.NativeOperation_STATEMENT, Value: normalized}}}
	unit := &protocol.Unit{ChangeId: "0", Body: &protocol.Unit_Native{Native: proto.CloneOf(value)}}
	return selected, change, unit
}

func TestRenderedNativeUnitRetainsCapturedOperation(t *testing.T) {
	c := qt.New(t)
	selected, change, unit := nativeRenderFixture()
	fragment, err := selected.renderedFragment(change, unit)
	c.Assert(err, qt.IsNil)
	operation := fragment.Native.(*resolvedOperation)
	c.Assert(fragment.SQL, qt.Equals, "")
	c.Assert(proto.Equal(operation.model.normalized, change.GetNative().Value), qt.IsTrue)
	c.Assert(operation.Effect(), qt.DeepEquals, schemaext.Effect{Impact: schemaext.Additive, Reason: "create"})
	c.Assert(operation.AccessEffect(), qt.DeepEquals, schemaext.AccessEffect{Access: schemaext.AccessUnchanged, Reason: "no access change"})
	unit.GetNative().Payload[0] = 'x'
	change.GetNative().Value.Value.Payload[0] = 'y'
	c.Assert(string(operation.model.wire.Payload), qt.Equals, `{"create":true}`)
	c.Assert(string(operation.model.normalized.Value.Payload), qt.Equals, `{"create":true}`)
}

func TestRenderedNativeUnitRefusesChangedOrUnboundEnvelope(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*target, *protocol.Change, *protocol.Unit)
	}{
		{"changed payload", func(_ *target, _ *protocol.Change, u *protocol.Unit) {
			u.GetNative().Payload = []byte(`{"create":false}`)
		}},
		{"changed owner", func(_ *target, _ *protocol.Change, u *protocol.Unit) { u.GetNative().Owner.Name = "other" }},
		{"common input", func(_ *target, c *protocol.Change, _ *protocol.Unit) {
			c.Operation = &protocol.Change_DropTable{DropTable: &protocol.Empty{}}
		}},
		{"missing model", func(s *target, _ *protocol.Change, _ *protocol.Unit) { s.host.models = nil }},
		{"different engine", func(s *target, _ *protocol.Change, _ *protocol.Unit) { s.engine.Id = "other" }},
		{"missing body", func(_ *target, _ *protocol.Change, u *protocol.Unit) { u.Body = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			selected, change, unit := nativeRenderFixture()
			test.mutate(selected, change, unit)
			fragment, err := selected.renderedFragment(change, unit)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(fragment, qt.DeepEquals, renderer.Fragment{})
		})
	}
}
