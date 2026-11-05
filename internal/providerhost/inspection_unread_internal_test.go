package providerhost

// White-box testing required: live database binding uses the private session
// receipt and negotiated identifier semantics before exposing a catalog.
import (
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

const unreadReason = "rep is a replica table no replication of this database writes any more"

// unreadSnapshot names one table app.rep the read leaves out.
func unreadSnapshot() *protocol.Schema {
	return &protocol.Schema{Engine: "fixture", UnreadTables: []*protocol.UnreadTable{{
		Table:      &protocol.Reference{Engine: "fixture", Database: "db", Schema: "app", Kind: "table", Name: "rep"},
		Diagnostic: &protocol.ProviderDiagnostic{Reason: unreadReason},
	}}}
}

func TestUnreadTablesBindDatabaseAndDefaultSchema_HappyPath(t *testing.T) {
	c := qt.New(t)
	tables, err := reservedReader().unreadTables(unreadSnapshot(), nil)
	c.Assert(err, qt.IsNil)
	c.Assert(tables, qt.HasLen, 1)
	c.Assert(tables[0].Identity.String(), qt.Equals, "table app.rep")
	c.Assert(tables[0].Reason, qt.Equals, unreadReason)
	none, err := reservedReader().unreadTables(&protocol.Schema{Engine: "fixture"}, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(none, qt.IsNil)
}

func TestUnreadTablesBindDatabaseAndDefaultSchema_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		change  func(*protocol.Schema)
		wantErr string
	}{
		{"another database", func(s *protocol.Schema) { s.UnreadTables[0].Table.Database = "foreign" },
			`.*an unread table belongs to another database`},
		{"a described table in the default schema", func(s *protocol.Schema) {
			ref := proto.CloneOf(s.UnreadTables[0].Table)
			ref.Schema = ""
			s.Tables = []*protocol.Table{{Identity: ref, NormalizedIdentity: proto.CloneOf(ref)}}
		}, `.*an unread table collides with another table after session binding`},
		{"two entries one table after binding", func(s *protocol.Schema) {
			other := proto.CloneOf(s.UnreadTables[0])
			other.Table.Schema = ""
			s.UnreadTables = append(s.UnreadTables, other)
		}, `.*an unread table collides with another table after session binding`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			snapshot := unreadSnapshot()
			test.change(snapshot)
			tables, err := reservedReader().unreadTables(snapshot, nil)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(tables, qt.IsNil)
		})
	}
}

// An unread table at a reserved name, which the SDK cannot see when the
// reservation spells the default schema another way, is refused once both
// are bound.
func TestUnreadTablesRefuseAReservedName(t *testing.T) {
	c := qt.New(t)
	r := reservedReader()
	reserved, err := r.reservedNames(reservedSnapshot())
	c.Assert(err, qt.IsNil)
	snapshot := unreadSnapshot()
	snapshot.UnreadTables[0].Table.Schema = ""
	snapshot.UnreadTables[0].Table.Name = "history"
	tables, err := r.unreadTables(snapshot, reserved)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*an unread table collides with another table after session binding`)
	c.Assert(tables, qt.IsNil)
}
