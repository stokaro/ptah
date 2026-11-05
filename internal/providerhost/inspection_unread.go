package providerhost

import (
	"fmt"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
)

// unreadTables binds the tables a read found and does not describe to the
// session's database and the common identifier rules. The SDK has checked
// each entry's shape and that the session negotiated the feature; what it
// cannot see is the session's database and the identity a table takes once
// the rules normalize it, so an entry of another database, or one that
// collides with another entry, a described table or a reservation after
// binding, is refused.
func (r reader) unreadTables(snapshot *protocol.Schema, reserved []catalog.ReservedName) ([]catalog.UnreadTable, error) {
	if len(snapshot.GetUnreadTables()) == 0 {
		return nil, nil
	}
	builder := objectidentity.NewBuilder(r.session.info.IdentifierSemantics)
	occupied := make(map[objectidentity.Key]bool, len(reserved)+len(snapshot.GetTables()))
	for _, record := range reserved {
		occupied[record.Identity.Key()] = true
	}
	for _, table := range snapshot.GetTables() {
		occupied[builder.TablePartsVerbatim(table.GetIdentity().GetSchema(), table.GetIdentity().GetName()).Key()] = true
	}
	result := make([]catalog.UnreadTable, 0, len(snapshot.GetUnreadTables()))
	seen := make(map[objectidentity.Key]bool, len(snapshot.GetUnreadTables()))
	for _, entry := range snapshot.GetUnreadTables() {
		table := entry.GetTable()
		if table.GetDatabase() != r.session.remote.GetDatabase() {
			return nil, fmt.Errorf("%w: an unread table belongs to another database", provider.ErrProtocol)
		}
		identity := builder.TablePartsVerbatim(table.GetSchema(), table.GetName())
		if occupied[identity.Key()] || seen[identity.Key()] {
			return nil, fmt.Errorf("%w: an unread table collides with another table after session binding", provider.ErrProtocol)
		}
		seen[identity.Key()] = true
		result = append(result, catalog.UnreadTable{Identity: identity, Reason: entry.GetDiagnostic().GetReason()})
	}
	return result, nil
}
