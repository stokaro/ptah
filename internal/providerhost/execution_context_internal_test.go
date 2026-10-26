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
	bound := &protocol.DatabaseContext{Database: "/target", SourceDatabase: "/source"}
	envelope := &protocol.Envelope{ProviderId: "example.org/fixture", Kind: "example.org/fixture/operation", SchemaVersion: 1, Encoding: "json", Context: bound}
	expected := []*protocol.Unit{{ChangeId: "0", Body: &protocol.Unit_Sql{Sql: "SELECT 1"}}, {ChangeId: "1", Body: &protocol.Unit_Native{Native: envelope}}}
	for _, row := range []struct {
		name    string
		session *protocol.DatabaseContext
		wantErr error
		want    []*protocol.Unit
	}{
		{name: "same", session: &protocol.DatabaseContext{Database: "/target", SourceDatabase: "/source"}, want: expected},
		{name: "different database", session: &protocol.DatabaseContext{Database: "/other", SourceDatabase: "/source"}, wantErr: provider.ErrProtocol},
		{name: "different authored root", session: &protocol.DatabaseContext{Database: "/target", SourceDatabase: "/other"}, wantErr: provider.ErrProtocol},
		{name: "missing session context", wantErr: provider.ErrProtocol},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			kind := schemaext.Kind("example.org/fixture/operation")
			subject := objectidentity.ID{Kind: "table", Name: objectidentity.Part{Source: "items", Normalized: "items"}}
			model := &resolvedModel{kind: kind, subject: subject, signature: "contract", canonical: []byte(`{}`), wire: modelWire{Format: 1, Engine: "fixture"}, normalized: &protocol.NormalizedValue{Value: envelope}}
			contract := &modelContract{key: modelKey{kind: kind, representation: schemaext.Operation}, signature: "contract"}
			session := &session{target: &target{engine: &protocol.Engine{Id: "fixture"}, host: &Host{models: map[modelKey]*modelContract{contract.key: contract}}}, remote: &protocol.Session{Context: row.session}}
			input := []renderer.Fragment{{SQL: "SELECT 1"}, {Native: &resolvedOperation{model: model}}}
			before := proto.CloneOf(envelope)
			units, fragments, err := session.encodeUnits(c.Context(), input)
			c.Assert(err, qt.ErrorIs, row.wantErr)
			c.Assert(units, qt.CmpEquals(protocmp.Transform()), row.want)
			c.Assert(fragments, qt.HasLen, len(row.want))
			c.Assert(envelope, qt.CmpEquals(protocmp.Transform()), before)
		})
	}
}
