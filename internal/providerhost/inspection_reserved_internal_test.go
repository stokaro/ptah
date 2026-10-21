package providerhost

// White-box testing required: live database binding uses the private session
// receipt and negotiated identifier semantics before exposing a catalog.
import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
)

func reservedReader() reader {
	return reader{session: &session{target: modelIdentityTarget(), remote: &protocol.Session{Engine: "fixture", Database: "db"}, info: catalog.ServerInfo{Dialect: "fixture", IdentifierSemantics: identifier.Semantics{DefaultSchema: "app", TableNames: identifier.ComparisonExact}}}}
}
func reservedSnapshot() *protocol.Schema {
	ref := &protocol.Reference{Engine: "fixture", Database: "db", Schema: "app", Kind: "table", Name: "history"}
	return &protocol.Schema{Engine: "fixture", ReservedObjects: []*protocol.ReservedObject{{Object: ref, NormalizedObject: proto.CloneOf(ref), Purpose: protocol.ReservedObject_HISTORY, ConflictKinds: []string{"table"}}}}
}
func TestReservedNamesBindDatabaseAndDefaultSchema(t *testing.T) {
	c := qt.New(t)
	r := reservedReader()
	snapshot := reservedSnapshot()
	names, err := r.reservedNames(snapshot)
	c.Assert(err, qt.IsNil)
	c.Assert(names, qt.HasLen, 1)
	c.Assert(names[0].ProviderID, qt.Equals, "example.org/fixture")
	snapshot.ReservedObjects[0].Object.Database = "foreign"
	_, err = r.reservedNames(snapshot)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	snapshot = reservedSnapshot()
	ref := proto.CloneOf(snapshot.ReservedObjects[0].Object)
	ref.Schema = ""
	snapshot.Tables = []*protocol.Table{{Identity: ref, NormalizedIdentity: proto.CloneOf(ref)}}
	_, err = r.reservedNames(snapshot)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	snapshot = reservedSnapshot()
	other := proto.CloneOf(snapshot.ReservedObjects[0])
	other.Object.Schema = ""
	other.NormalizedObject.Schema = ""
	snapshot.ReservedObjects = append(snapshot.ReservedObjects, other)
	_, err = r.reservedNames(snapshot)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
}
