package providerhost

// White-box testing required: unit preparation must reject a foreign database
// before any RPC. A session without a client makes that boundary observable.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"ptah.run/core/objectidentity"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
)

func TestNativeUnitPreparationRequiresExactBoundSessionContext(t *testing.T) {
	for _, row := range []struct {
		name    string
		session *protocol.DatabaseContext
		refused bool
	}{
		{name: "same", session: &protocol.DatabaseContext{Database: "/target", SourceDatabase: "/source"}},
		{name: "different database", session: &protocol.DatabaseContext{Database: "/other", SourceDatabase: "/source"}, refused: true},
		{name: "different authored root", session: &protocol.DatabaseContext{Database: "/target", SourceDatabase: "/other"}, refused: true},
		{name: "missing session context", refused: true},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			kind := schemaext.Kind("example.org/fixture/operation")
			subject := objectidentity.ID{Kind: objectidentity.Table, Name: objectidentity.Part{Source: "items", Normalized: "items"}}
			bound := &protocol.DatabaseContext{Database: "/target", SourceDatabase: "/source"}
			envelope := &protocol.Envelope{ProviderId: "example.org/fixture", Kind: string(kind), SchemaVersion: 1, Encoding: "json", Context: bound}
			model := &resolvedModel{kind: kind, subject: subject, signature: "contract", canonical: []byte(`{}`), wire: modelWire{Format: 1, Engine: "fixture"}, normalized: &protocol.NormalizedValue{Value: envelope}}
			contract := &modelContract{key: modelKey{kind: kind, representation: schemaext.Operation}, signature: "contract"}
			session := &session{target: &target{engine: &protocol.Engine{Id: "fixture"}, host: &Host{models: map[modelKey]*modelContract{contract.key: contract}}}, remote: &protocol.Session{Context: row.session}}
			input := []renderer.Fragment{{SQL: "SELECT 1"}, {Native: &resolvedOperation{model: model}}}
			units, fragments, err := session.encodeUnits(c.Context(), input)
			if row.refused {
				c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
				c.Assert(units, qt.IsNil)
				c.Assert(fragments, qt.IsNil)
				return
			}
			c.Assert(err, qt.IsNil)
			c.Assert(units, qt.HasLen, 2)
			c.Assert(units[1].GetNative().GetContext(), qt.CmpEquals(protocmp.Transform()), bound)
			c.Assert(proto.Equal(envelope.GetContext(), bound), qt.IsTrue)
		})
	}
}
